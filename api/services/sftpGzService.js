const Client = require('ssh2-sftp-client');
const { pipeline } = require('stream/promises');
const fs = require('fs');
const zlib = require('zlib');
const path = require('path');
const { mkTempFile } = require('../config/utils'); // Asumiendo que esta es la ruta correcta
const { obtenerNombresArchivosProcesados } = require('../models/logArchivoModel'); // Asumiendo que esta es la ruta correcta

const sftp = new Client();

/**
 * Conecta al servidor SFTP.
 */
async function conectarSFTP() {
  await sftp.connect({
    host: process.env.SSH_HOST,
    port: process.env.SSH_PORT,
    username: process.env.SSH_USER,
    password: process.env.SSH_PASSWORD,
  });
}

/**
 * Cierra la conexión SFTP de manera segura.
 */
async function cerrarSFTP() {
  try {
    await sftp.end();
    console.log('[SFTP] Conexión SFTP cerrada correctamente');
  } catch (cerrarError) {
    console.warn('Error al cerrar conexión SFTP:', cerrarError.message);
  }
}

/**
 * Lista los archivos .dat y .dat.gz nuevos en el directorio remoto.
 * Un archivo se considera "nuevo" si no está en el listado de archivos procesados.
 */
async function listarArchivosNuevos() {
  const remoteDir = process.env.REMOTE_DAT_DIR;
  const archivosRemotos = await sftp.list(remoteDir);
  const archivosProcesados = await obtenerNombresArchivosProcesados();
  const re = /\.(dat|DAT)(\.gz)?$/;

  const nuevosArchivos = archivosRemotos
    .filter((item) => re.test(item.name)) // Filtra por extensión .dat o .dat.gz
    .filter((item) => !archivosProcesados.includes(item.name)) // Solo nuevos
    .map((item) => ({
      nombre: item.name,
      ruta: path.posix.join(remoteDir, item.name),
      esGz: item.name.toLowerCase().endsWith('.gz'),
      size: item.size,
    }));

  return nuevosArchivos;
}

/**
 * Descarga un archivo .dat.
 * @param {string} remotePath - La ruta remota del archivo.
 * @returns {object} Un objeto con el directorio temporal y la ruta local del archivo.
 */
async function descargarDat(remotePath) {
  const { dir, file } = mkTempFile('.dat');
  await sftp.fastGet(remotePath, file);
  return { dir, localPath: file };
}

/**
 * Descarga y descomprime un archivo .gz de forma robusta.
 * @param {string} remotePath - La ruta remota del archivo.
 * @returns {object} Un objeto con el directorio temporal y la ruta local del archivo descomprimido.
 */
async function descargarYGunzip(remotePath) {
  const { dir, file } = mkTempFile('.dat');

  // Descarga el archivo completo como un Buffer
  const fileBuffer = await sftp.get(remotePath);

  // Asegúrate de que el buffer no esté vacío
  if (!fileBuffer || fileBuffer.length === 0) {
    throw new Error('El archivo remoto está vacío o no se pudo descargar.');
  }

  // Descomprime el Buffer
  const decompressedBuffer = zlib.gunzipSync(fileBuffer);

  // Guarda el buffer descomprimido en el archivo temporal
  fs.writeFileSync(file, decompressedBuffer);

  return { dir, localPath: file };
}

module.exports = {
  listarArchivosNuevos,
  descargarDat,
  descargarYGunzip,
  conectarSFTP,
  cerrarSFTP,
};
