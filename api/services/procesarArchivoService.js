const oracledb = require('oracledb');
const path = require('path');
const SftpClient = require('ssh2-sftp-client');
const parserCCN = require('../config/parserCCN');
const parserCDN = require('../config/parserCDN');
const { insertarRegistroCuadratura } = require('../models/cuadraturaModel');

const obtenerParserPorTipo = (tipo) => {
  switch (tipo) {
    case 'CCN':
      return parserCCN;
    case 'CDN':
      return parserCDN;
    default:
      throw new Error('Tipo de archivo no soportado: ' + tipo);
  }
};

const procesarArchivoRemoto = async (nombreArchivo) => {
  const sftp = new SftpClient();
  const tipo = nombreArchivo.split('_')[0];
  const parser = obtenerParserPorTipo(tipo);

  const remotePath = path.join(process.env.REMOTE_DAT_DIR, nombreArchivo);

  try {
    await sftp.connect({
      host: process.env.SSH_HOST,
      port: process.env.SSH_PORT,
      username: process.env.SSH_USER,
      password: process.env.SSH_PASSWORD,
    });

    const buffer = await sftp.get(remotePath);
    const contenido = buffer.toString('utf-8');
    const lineas = contenido.split('\n').filter(Boolean);

    const connection = await oracledb.getConnection({
      user: process.env.ORACLE_USER,
      password: process.env.ORACLE_PASSWORD,
      connectString: process.env.ORACLE_CONNECT_STRING,
    });

    let registrosInsertados = 0;

    for (const linea of lineas) {
      const tipoLinea = linea.slice(0, 2).trim();

      // Saltar encabezado o pie de archivo
      if (tipoLinea === 'HR' || tipoLinea === 'TR') continue;

      const registro = parser(linea);
      registro.ID = Date.now() + Math.floor(Math.random() * 1000);

      await insertarRegistroCuadratura(connection, registro, nombreArchivo);
      registrosInsertados++;
    }

    await connection.commit();
    await connection.close();
    return { mensaje: `${registrosInsertados} registros insertados desde ${nombreArchivo}` };
  } catch (error) {
    throw new Error('Error al procesar archivo: ' + error.message);
  } finally {
    sftp.end();
  }
};

module.exports = {
  procesarArchivoRemoto,
};
