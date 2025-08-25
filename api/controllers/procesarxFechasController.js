const { ejecutarScriptRemoto } = require('../services/sshService');
const { listarArchivosDatNuevos } = require('../services/sftpService');
const { leerYParsearArchivo } = require('../services/parserService');
const { insertarRegistros } = require('../services/insertService');
const { getConnection, obtenerFechaDesdeNombre } = require('../config/utils');
const { archivoYaProcesado } = require('../services/insertxFechasService');

const procesarArchivosPorFecha = async (req, res) => {
  const { fechaInicio, fechaFin } = req.body;

  const resultados = [];
  const errores = [];

  const connection = await getConnection();

  try {
    // Paso 1: Ejecutar script remoto si se solicita
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

    // Paso 2: Listar archivos disponibles
    let archivos = await listarArchivosDatNuevos();

    // Paso 3: Filtro por tipo si se indica
    archivos = archivos.filter((a) => !a.nombre.startsWith('LDN') && !a.nombre.startsWith('LCN'));

    // Paso 4: Filtro por fechas reales en nombre si se indican
    if (fechaInicio && fechaFin) {
      const fi = new Date(fechaInicio);
      const ff = new Date(fechaFin);
      archivos = archivos.filter((a) => {
        const fecha = obtenerFechaDesdeNombre(a.nombre);
        return fecha && fecha >= fi && fecha <= ff;
      });
    }

    if (archivos.length === 0) {
      return res.json({ mensaje: 'No hay archivos para procesar tras los filtros aplicados.' });
    }

    // Paso 5: Filtrar archivos que ya fueron procesados
    const archivosValidos = [];
    for (const archivo of archivos) {
      const yaExiste = await archivoYaProcesado(connection, archivo.nombre);
      if (!yaExiste) {
        archivosValidos.push(archivo);
      } else {
        console.log(`[SKIP] Archivo ya procesado: ${archivo.nombre}`);
      }
    }

    if (archivosValidos.length === 0) {
      return res.json({ mensaje: 'Todos los archivos encontrados ya fueron procesados.' });
    }

    // Paso 6: Procesar archivos válidos
    for (const archivo of archivosValidos) {
      try {
        const { tipo, registros } = await leerYParsearArchivo(archivo.nombre);

        const resultado = await insertarRegistros({
          tipo,
          registros,
          nombreArchivo: archivo.nombre,
        });

        resultados.push({
          archivo: archivo.nombre,
          tipo,
          cantidadInsertada: resultado.cantidad,
        });
      } catch (error) {
        errores.push({
          archivo: archivo.nombre,
          mensaje: error.message,
        });
      }
    }

    res.json({
      mensaje: `Proceso finalizado. Archivos procesados: ${resultados.length}, con errores: ${errores.length}`,
      resultados,
      errores,
    });
  } catch (error) {
    console.error('Error general:', error);
    res.status(500).json({ mensaje: 'Error general del proceso', detalle: error.message });
  } finally {
    if (connection) {
      await connection.close();
    }
  }
};

module.exports = {
  procesarArchivosPorFecha,
};
