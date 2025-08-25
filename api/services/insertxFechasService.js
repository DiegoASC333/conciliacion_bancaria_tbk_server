//const { getConnection } = require('../config/utils');

async function archivoYaProcesado(connection, nombreArchivo) {
  const sql = `
    SELECT COUNT(1) as cantidad
    FROM LOG_ARCHIVOS_PROCESADOS
    WHERE NOMBRE_ARCHIVO = :nombre
  `;
  const result = await connection.execute(sql, { nombre: nombreArchivo });
  return result.rows[0].CANTIDAD > 0;
}

module.exports = {
  archivoYaProcesado,
};
