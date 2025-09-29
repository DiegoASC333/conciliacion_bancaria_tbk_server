const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function ejecutarReprocesoCupon(cupon, id) {
  const sql = `BEGIN REPROCESA_CUPON(:pCupon, :pId, :pResultado ); END;`;

  const binds = {
    pCupon: { val: cupon, dir: oracledb.BIND_IN, type: oracledb.NUMBER },
    pId: { val: id, dir: oracledb.BIND_IN, type: oracledb.NUMBER },
    pResultado: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 4000 },
  };

  let conn;
  try {
    conn = await getConnection();
    const r = await conn.execute(sql, binds);
    await conn.commit();

    const resultadoOracle = r.outBinds.pResultado;
    const estadoJson = JSON.parse(resultadoOracle);

    return { ok: true, estado: estadoJson };
  } catch (error) {
    if (conn) await conn.rollback();
    throw error;
  } finally {
    if (conn) await conn.close();
  }
}

module.exports = { ejecutarReprocesoCupon };
