const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function ejecutarReprocesoCupon(cupon) {
  const sql = `BEGIN REPROCESA_CUPON(:pCupon); END;`;

  const binds = {
    pCupon: { val: cupon, dir: oracledb.BIND_IN, type: oracledb.NUMBER },
  };

  let conn;
  try {
    conn = await getConnection();
    const r = await conn.execute(sql, binds);
    await conn.commit();
    return { ok: true, estado: r };
  } catch (error) {
    if (conn) await conn.rollback();
    throw error;
  } finally {
    if (conn) await conn.close();
  }
}

module.exports = { ejecutarReprocesoCupon };
