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

    const resultadoOracle = JSON.parse(r.outBinds.pResultado);

    if (resultadoOracle.success === true) {
      await conn.commit();
      return { ok: true, estado: resultadoOracle };
    } else {
      await conn.rollback();
      return { ok: false, estado: resultadoOracle };
    }
  } catch (error) {
    if (conn) await conn.rollback();
    throw error;
  } finally {
    if (conn) await conn.close();
  }
}

module.exports = { ejecutarReprocesoCupon };
