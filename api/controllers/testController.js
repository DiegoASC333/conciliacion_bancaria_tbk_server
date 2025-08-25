/* eslint-disable no-undef */
const { ejecutarScriptRemoto } = require('../services/sshService');
const { listarArchivosDatNuevos } = require('../services/sftpService');
const { leerYParsearArchivo } = require('../services/parserService');
const { insertarRegistros } = require('../services/insertService');

const testProcesoCadena = async (req, res) => {
  const resultados = [];
  const errores = [];

  // Paso 1: Ejecutar script remoto (opcional)
  try {
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
  } catch (e) {
    return res.status(500).json({ mensaje: 'Fallo en conexión SSH', error: e.output });
  }

  // Paso 2: Listar archivos nuevos
  let archivos;
  try {
    archivos = await listarArchivosDatNuevos();

    archivos = archivos.filter(
      (a) =>
        !a.nombre.startsWith('CCN') && !a.nombre.startsWith('LDN') && !a.nombre.startsWith('LCN')
    );

    if (archivos.length === 0) {
      return res.json({ mensaje: 'No hay archivos nuevos para procesar.' });
    }
  } catch (e) {
    return res.status(500).json({ mensaje: 'Error al listar archivos nuevos', error: e.message });
  }

  // Limitar a 10 archivos para el test
  const archivosProcesar = archivos.slice(0, 10);

  // Paso 3 y 4: Leer, parsear e insertar cada archivo
  for (const archivo of archivosProcesar) {
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

      console.log('[TestProcesoCadena] Proceso completo. Fin del controlador.');
    } catch (error) {
      errores.push({
        archivo: archivo.nombre,
        mensaje: error.message,
      });
    } //agregar finally
  }

  res.json({
    mensaje: `Proceso finalizado. Archivos procesados: ${resultados.length}, con errores: ${errores.length}`,
    resultados,
    errores,
  });
};

module.exports = {
  testProcesoCadena,
};
