const SftpClient = require('ssh2-sftp-client');
const path = require('path');
const { detectarTipoArchivo } = require('../config/detectFileType');
const parseCCN = require('../config/parseoCCN');
const parseCDN = require('../config/parseoCDN');
const parseLCN = require('../config/parseoLCN');
const parseLDN = require('../config/parseoLDN');
// const { generarID } = require('../config/utils');

//const sftp = new SftpClient();

const PARSERS = {
  CCN: parseCCN,
  CDN: parseCDN,
  LCN: parseLCN,
  LDN: parseLDN,
};

/**
 * Lee un archivo completo desde SFTP y lo convierte en objetos parseados
 */
async function leerYParsearArchivo(nombreArchivo) {
  const sftp = new SftpClient();
  const remotePath = `${process.env.REMOTE_DAT_DIR}${nombreArchivo}`;

  try {
    await sftp.connect({
      host: process.env.SSH_HOST,
      port: process.env.SSH_PORT,
      username: process.env.SSH_USER,
      password: process.env.SSH_PASSWORD,
    });

    const fileBuffer = await sftp.get(remotePath);
    const contenido = fileBuffer.toString('utf-8');
    const lineas = contenido.split(/\r?\n/).filter((line) => line.trim() !== '');

    if (lineas.length < 3) {
      throw new Error('El archivo no contiene suficiente contenido útil.');
    }

    const tipo = detectarTipoArchivo(lineas[0], nombreArchivo);
    const parser = PARSERS[tipo];

    if (!parser) {
      throw new Error(`Parser no definido para tipo de archivo: ${tipo}`);
    }

    const lineasDetalle = lineas.slice(1, -1); // quitar header y footer
    //const registros = lineasDetalle.map(parser).filter(r => !!r);
    const registros = lineasDetalle
      .map(parser)
      .filter((r) => !!r)
      .map((r) => {
        const limpio = {};
        for (const key in r) {
          const val = r[key];
          // Si es string vacío para número, lo dejamos como null
          limpio[key] = typeof val === 'string' && val.trim() === '' ? null : val;
        }

        return {
          // ID: generarID(),
          TIPO_TRANSACCION: tipo,
          FILE_NAME: nombreArchivo,
          STATUS_SAP_REGISTER: 'PENDIENTE',
          STATUS_SAP_DATE: new Date(),
          DATE_LOAD_BBDD: new Date(),
          ...limpio,
        };
      });

    return { tipo, registros };
  } catch (error) {
    console.error('Error al leer o parsear archivo:', error);
    throw error;
  } finally {
    sftp.end();
  }
}

module.exports = {
  leerYParsearArchivo,
};
