const SftpClient = require('ssh2-sftp-client');
const { obtenerNombresArchivosProcesados } = require('../models/logArchivoModel');

const sftp = new SftpClient();

async function conectarSFTP() {
  await sftp.connect({
    host: process.env.SSH_HOST,
    port: process.env.SSH_PORT,
    username: process.env.SSH_USER,
    password: process.env.SSH_PASSWORD,
  });
}

async function listarArchivosDatNuevos() {
  const remoteDir = process.env.REMOTE_DAT_DIR;

  try {
    await conectarSFTP();
    const archivosRemotos = await sftp.list(remoteDir);
    const archivosProcesados = await obtenerNombresArchivosProcesados();

    const nuevosArchivos = archivosRemotos
      .filter((file) => file.name.endsWith('.dat'))
      .filter((file) => !archivosProcesados.includes(file.name)) // solo nuevos
      .map((file) => ({
        nombre: file.name,
        tamaño: file.size,
        fecha: file.modifyTime,
      }));

    return nuevosArchivos;
  } catch (error) {
    console.error('Error al filtrar archivos nuevos:', error);
    throw error;
  } finally {
    try {
      await sftp.end();
      console.log('[SFTP] Conexión SFTP cerrada correctamente');
    } catch (cerrarError) {
      console.warn('Error al cerrar conexión SFTP:', cerrarError.message);
    }
  }
}

module.exports = {
  listarArchivosDatNuevos,
};
