const oracledb = require('oracledb');
const { getConnection } = require('../config/utils'); // tu conexión Oracle

async function obtenerNombresArchivosProcesados() {
  const connection = await getConnection();
  const result = await connection.execute(
    `SELECT NOMBRE_ARCHIVO FROM LOG_ARCHIVOS_PROCESADOS WHERE FECHA_CARGA > SYSDATE - 30`,
    [],
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  await connection.close();

  return result.rows.map((row) => row.NOMBRE_ARCHIVO);
}

module.exports = {
  obtenerNombresArchivosProcesados,
};
