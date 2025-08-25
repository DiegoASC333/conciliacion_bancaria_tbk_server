const { ejecutarScriptRemoto } = require('../services/sshService');
const {
  listarArchivosNuevos,
  descargarDat,
  descargarYGunzip,
  conectarSFTP,
  cerrarSFTP,
} = require('../services/sftpGzService'); // El servicio unificado que creamos
const { procesarCupones } = require('../services/procesarCuponesService');
const { leerYParsearArchivoLocal } = require('../services/parserLocalAdapter');
const { insertarRegistros, verificarArchivoProcesado } = require('../services/insertService');
const { stripGz, cleanupTemp, obtenerFechaDesdeNombre } = require('../config/utils');

const procesarArchivosRemotosAutomatico = async (req, res) => {
  const resultados = [];
  const errores = [];

  try {
    // 1) Opcional: Ejecutar script remoto
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

    // -------------------------------------------------------------
    // ✨ LÓGICA AGREGADA: Generar el rango de fechas automáticamente
    const fechaFin = new Date();
    const fechaInicio = new Date();
    fechaInicio.setDate(fechaInicio.getDate() - 4);
    // -------------------------------------------------------------

    // 2) Listar archivos .dat y .dat.gz nuevos (con la lógica del servicio unificado)
    let archivos = await listarArchivosNuevos();

    // 3) Filtro por fechas reales en nombre (siempre se aplica)
    archivos = archivos.filter((a) => {
      const nombreLogico = stripGz(a.nombre);
      const fecha = obtenerFechaDesdeNombre(nombreLogico);
      // Incluir lógica de comparación de fechas
      return fecha && fecha >= fechaInicio && fecha <= fechaFin;
    });

    if (archivos.length === 0) {
      return res.json({ mensaje: 'No hay archivos para procesar tras los filtros aplicados.' });
    }

    // 4) Procesar cada archivo individualmente
    for (const archivo of archivos) {
      const nombreOriginal = archivo.nombre;
      const nombreLogico = stripGz(nombreOriginal);

      try {
        // Idempotencia por NOMBRE_ARCHIVO lógico
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

        // Descargar (y descomprimir si corresponde)
        const { dir, localPath } = archivo.esGz
          ? await descargarYGunzip(archivo.ruta)
          : await descargarDat(archivo.ruta);

        try {
          const { tipo, registros } = await leerYParsearArchivoLocal({
            rutaLocal: localPath,
            nombreArchivoLogico: nombreLogico,
          });

          const r = await insertarRegistros({ tipo, registros, nombreArchivo: nombreLogico });

          if (r.exito) {
            console.log('Inserción de registros principales exitosa. Procesando cupones...');
            await procesarCupones();
            console.log('Procesamiento de cupones finalizado.');
          }

          resultados.push({
            archivoOriginal: nombreOriginal,
            archivoUsado: nombreLogico,
            tipo,
            estado: r?.estado === 'omitido' ? 'omitido' : 'procesado',
            cantidadInsertada: r?.cantidad ?? 0,
          });
        } finally {
          cleanupTemp(dir);
        }
      } catch (e) {
        errores.push({
          archivoOriginal: nombreOriginal,
          archivoUsado: nombreLogico,
          error: e.message,
        });
      }
    }

    res.json({
      mensaje: `Proceso finalizado. Archivos procesados: ${resultados.filter((x) => x.estado === 'procesado').length}, con errores: ${errores.length}`,
      resultados,
      errores,
    });
  } catch (error) {
    console.error('Error general del proceso:', error);
    res.status(500).json({ mensaje: 'Error general del proceso', detalle: error.message });
  } finally {
    await cerrarSFTP();
  }
};

module.exports = {
  procesarArchivosRemotosAutomatico,
};
