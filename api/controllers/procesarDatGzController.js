const { ejecutarScriptRemoto } = require('../services/sshService');
const {
  listarArchivosNuevos,
  descargarDat,

  descargarYGunzip,
  conectarSFTP,
  cerrarSFTP,
} = require('../services/sftpGzService');
const { procesarCupones } = require('../services/procesarCuponesService');
const { leerYParsearArchivoLocal } = require('../services/parserLocalAdapter');
const { insertarRegistros, verificarArchivoProcesado } = require('../services/insertService');
const { stripGz, cleanupTemp, obtenerFechaDesdeNombre } = require('../config/utils');
const { ejecutarValidaCupon } = require('../services/validaCuponService');
const { obtenerNombresSAP } = require('../services/consultaClienteService'); //servicio para consultar a cliente

const procesarArchivosRemotos = async (req, res) => {
  const { fechaInicio, fechaFin } = req.body || {};

  const resultados = [];
  const errores = [];
  let huboInserciones = false; // ⬅️ NUEVO
  let postproc = {
    // ⬅️ NUEVO
    procesarCupones: null,
    validaCupon: null,
  };

  try {
    // 1) Script remoto
    const { code, output } = await ejecutarScriptRemoto({
      host: process.env.SSH_HOST,
      port: process.env.SSH_PORT,
      username: process.env.SSH_USER,
      password: process.env.SSH_PASSWORD,
      rutaScript: process.env.REMOTE_SCRIPT,
    });
    if (code !== 0) {
      return res.status(500).json({ mensaje: 'Error en script remoto', detalle: output });
    }

    await conectarSFTP();

    let archivos = await listarArchivosNuevos();

    // 3) Filtro por fechas (INCLUSIVO)
    if (fechaInicio && fechaFin) {
      const fi = new Date(fechaInicio);
      const ff = new Date(fechaFin);
      // incluir todo el día de fechaFin
      ff.setHours(23, 59, 59, 999);

      archivos = archivos.filter((a) => {
        const nombreLogico = stripGz(a.nombre);
        const fecha = obtenerFechaDesdeNombre(nombreLogico);
        return fecha && fecha >= fi && fecha <= ff;
      });
    }

    if (archivos.length === 0) {
      return res.json({ mensaje: 'No hay archivos para procesar tras los filtros aplicados.' });
    }

    // 4) Procesar cada archivo
    for (const archivo of archivos) {
      const nombreOriginal = archivo.nombre;
      const nombreLogico = stripGz(nombreOriginal);

      try {
        // Idempotencia
        const ya = await verificarArchivoProcesado(nombreLogico);
        if (ya) {
          resultados.push({
            archivoOriginal: nombreOriginal,
            archivoUsado: nombreLogico,
            estado: 'omitido',
            motivo: 'ya_procesado',
          });
          continue;
        }

        // Descargar / descomprimir
        const descarga = archivo.esGz
          ? await descargarYGunzip(archivo.ruta)
          : await descargarDat(archivo.ruta);

        const { dir, localPath } = descarga || {};

        try {
          // Parseo
          const { tipo, registros } = await leerYParsearArchivoLocal({
            rutaLocal: localPath,
            nombreArchivoLogico: nombreLogico,
          });

          // Insertar
          const r = await insertarRegistros({ tipo, registros, nombreArchivo: nombreLogico });

          if (r?.exito && (r?.cantidad ?? 0) > 0) {
            huboInserciones = true; // ⬅️ MARCAMOS
          }

          resultados.push({
            archivoOriginal: nombreOriginal,
            archivoUsado: nombreLogico,
            tipo,
            estado: r?.estado === 'omitido' ? 'omitido' : 'procesado',
            cantidadInsertada: r?.cantidad ?? 0,
          });
        } finally {
          if (dir) cleanupTemp(dir); // ⬅️ proteger cleanup
        }
      } catch (e) {
        errores.push({
          archivoOriginal: nombreOriginal,
          archivoUsado: nombreLogico,
          error: e.message,
        });
      }
    }

    // 5) Post-proceso global (una sola vez, BEST-EFFORT)
    if (huboInserciones) {
      // a) procesarCupones
      try {
        await procesarCupones();
        postproc.procesarCupones = { ok: true };
      } catch (e) {
        postproc.procesarCupones = { ok: false, error: e.message || String(e) };
        console.error('[procesarCupones] ERROR:', e);
      }

      // b) valida_cupon
      try {
        const v = await ejecutarValidaCupon(); // sin IN/OUT
        postproc.validaCupon = v.ok ? { ok: true } : { ok: false, error: v.error };
        if (!v.ok) console.error('[valida_cupon] ERROR:', v.error);
      } catch (e) {
        postproc.validaCupon = { ok: false, error: e.message || String(e) };
        console.error('[valida_cupon] Exception:', e);
      }
    }

    try {
      const archivosProcesados = resultados
        .filter((r) => r.estado === 'procesado')
        .map((r) => r.archivoUsado);

      const resultadoEnriquecimiento = await obtenerNombresSAP(archivosProcesados);
      postproc.enriquecimientoNombres = { ok: true, ...resultadoEnriquecimiento };
    } catch (e) {
      console.error('[enriquecimientoNombres] Exception:', e);
      postproc.enriquecimientoNombres = { ok: false, error: e.message || String(e) };
    }

    res.json({
      mensaje: `Proceso finalizado. Archivos procesados: ${
        resultados.filter((x) => x.estado === 'procesado').length
      }, con errores: ${errores.length}`,
      resultados,
      errores,
      postproc, // ⬅️ trazabilidad del post-proceso
    });
  } catch (error) {
    console.error('Error general del proceso:', error);
    res.status(500).json({ mensaje: 'Error general del proceso', detalle: error.message });
  } finally {
    await cerrarSFTP();
  }
};

module.exports = {
  procesarArchivosRemotos,
};
