const oracledb = require('oracledb');
const { getConnection } = require('../config/utils');

async function obtenerUltimosRegistrosDB(prefijos) {
  const connection = await getConnection();
  const sql = `
    SELECT TIPO_ARCHIVO, NOMBRE_ARCHIVO FROM (
      SELECT TIPO_ARCHIVO, NOMBRE_ARCHIVO, 
             ROW_NUMBER() OVER (PARTITION BY TIPO_ARCHIVO ORDER BY ID DESC) as rnk
      FROM LOG_ARCHIVOS_PROCESADOS
      WHERE TIPO_ARCHIVO IN ('LCN', 'LDN') AND ESTADO = 'PROCESADO'
    ) WHERE rnk = 1`;

  const result = await connection.execute(sql, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
  await connection.close();
  return result.rows;
}

module.exports = {
  obtenerUltimosRegistrosDB,
};
