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
const { obtenerNombresSAP } = require('../services/consultaClienteService');

const procesarArchivosRemotosPorNombre = async (req, res) => {
  // 1. Extraemos los nuevos parámetros del body
  const { fechaInicio, fechaFin, nombreArchivoFiltro, tiposAProcesar } = req.body || {};

  const resultados = [];
  const errores = [];
  let huboInserciones = false;
  let postproc = {
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

    // -----------------------------------------------------------------
    // INICIO: Bloque de Filtros Corregido
    // -----------------------------------------------------------------

    // 3) Filtro por nombre de archivo (TIENE PRIORIDAD)
    if (nombreArchivoFiltro) {
      archivos = archivos.filter((a) => {
        const nombreLogico = stripGz(a.nombre);
        // Comparamos tanto el nombre original como el lógico
        return a.nombre === nombreArchivoFiltro || nombreLogico === nombreArchivoFiltro;
      });

      // 4) Filtro por fechas (SOLO SI NO SE FILTRÓ POR NOMBRE)
    } else if (fechaInicio && fechaFin) {
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

    // 5) Filtro por tipos de archivo (se aplica al resultado de 3 o 4)
    if (tiposAProcesar && Array.isArray(tiposAProcesar) && tiposAProcesar.length > 0) {
      archivos = archivos.filter((a) => {
        const nombreLogico = stripGz(a.nombre);
        // Asumimos que el tipo son los 3 primeros caracteres del nombre lógico (ej: "LCN" en "LCN_...")
        const tipoArchivo = nombreLogico.substring(0, 3).toUpperCase();
        return tiposAProcesar.includes(tipoArchivo);
      });
    }

    // -----------------------------------------------------------------
    // FIN: Bloque de Filtros Corregido
    // -----------------------------------------------------------------

    // 6) Comprobación de archivos restantes
    if (archivos.length === 0) {
      return res.json({ mensaje: 'No hay archivos para procesar tras los filtros aplicados.' });
    }

    // 7) Procesar cada archivo
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

    // 8) Post-proceso global (una sola vez, BEST-EFFORT)
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
      postproc,
    });
  } catch (error) {
    console.error('Error general del proceso:', error);
    res.status(500).json({ mensaje: 'Error general del proceso', detalle: error.message });
  } finally {
    await cerrarSFTP();
  }
};

module.exports = {
  procesarArchivosRemotosPorNombre,
};
