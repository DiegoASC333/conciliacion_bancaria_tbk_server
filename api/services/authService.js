const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function encontrarUsuario({ rut }) {
  let connection;
  const sql = `SELECT 
        A.RUT AS "rut", 
        B.NOMBRE_PILA || ' ' || B.APELLIDO_PATERNO || ' ' || B.APELLIDO_MATERNO AS "nombre", 
        A.ROL AS "rol", 
        A.PERFIL AS "perfil", 
        A.ACTIVO AS "activo" 
    FROM 
        USUARIOS A
    JOIN 
        utsap001.rem_ficha_sap2 B ON A.RUT = B.ID_PERSONA
    WHERE 
        A.RUT = :rut`;

  try {
    connection = await getConnection();
    const result = await connection.execute(
      sql,
      { rut },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    return result.rows.length > 0 ? result.rows[0] : null;
  } catch (error) {
    console.error('Error en el servicio al buscar usuario por RUT:', error);
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error('Error al cerrar la conexión:', err);
      }
    }
  }
}

module.exports = {
  encontrarUsuario,
};
