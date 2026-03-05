const path = require('path');
const {
  listarArchivosNuevos,
  descargarDat,
  descargarYGunzip,
  conectarSFTP,
  cerrarSFTP,
} = require('../services/sftpGzService');
const { insertarRegistros } = require('../services/insertService');
const { stripGz, cleanupTemp } = require('../config/utils');
const { leerYParsearArchivoLocal } = require('../services/parserLocalAdapter');

async function procesarArchivosPorTipo({ prefijos, filtroDinamico, callbackPostProceso }) {
  const resultados = [];
  const errores = [];
  let huboInserciones = false;

  try {
    // 1. Preparar ambiente
    // await ejecutarScriptRemoto({
    //   host: process.env.SSH_HOST,
    //   port: process.env.SSH_PORT,
    //   username: process.env.SSH_USER,
    //   password: process.env.SSH_PASSWORD,
    //   rutaScript: process.env.REMOTE_SCRIPT,
    // });
    await conectarSFTP();

    // 2. Obtener lista y filtrar
    let archivos = await listarArchivosNuevos();

    if (filtroDinamico) {
      archivos = archivos.filter((a) => filtroDinamico(a.nombre));
    } else {
      archivos = archivos.filter((a) => prefijos.some((p) => a.nombre.startsWith(p)));
    }

    // 3. Bucle de procesamiento (Descarga -> Parsea -> Inserta)
    for (const archivo of archivos) {
      const nombreLogico = stripGz(archivo.nombre);
      try {
        const { dir, localPath } = archivo.esGz
          ? await descargarYGunzip(archivo.ruta)
          : await descargarDat(archivo.ruta);
        try {
          const { tipo, registros } = await leerYParsearArchivoLocal({
            rutaLocal: localPath,
            nombreArchivoLogico: nombreLogico,
          });
          const r = await insertarRegistros({ tipo, registros, nombreArchivo: nombreLogico });

          if (r?.exito) huboInserciones = true;
          resultados.push({
            archivo: nombreLogico,
            tipo,
            estado: 'procesado',
            cantidad: r?.cantidad ?? 0,
          });
        } finally {
          if (dir) cleanupTemp(dir);
        }
      } catch (e) {
        errores.push({ archivo: nombreLogico, error: e.message });
      }
    }

    // 4. Post-proceso (Solo si hubo datos nuevos)
    let postProcData = null;
    if (huboInserciones && callbackPostProceso) {
      postProcData = await callbackPostProceso();
    }

    return { resultados, errores, postProcData };
  } finally {
    await cerrarSFTP();
  }
}

module.exports = {
  procesarArchivosPorTipo,
};
