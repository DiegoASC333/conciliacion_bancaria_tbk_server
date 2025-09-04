// api/services/validaCuponService.js
const oracledb = require('oracledb');
const { getConnection } = require('../config/utils');

/**
 * Ejecuta el procedimiento PL/SQL valida_cupon.
 * Ajusta el bloque PL/SQL si tu proc está dentro de un paquete (p.ej. PKG_TBK.valida_cupon).
 */
async function ejecutarValidaCupon() {
  let conn;
  try {
    conn = await getConnection();

    const plsql = `
      BEGIN
        valida_cupon;
      END;`;

    // Si no tienes OUT binds, puedes ejecutar sin binds:
    await conn.execute(plsql, {}, { autoCommit: false });

    // Si el proc no es AUTONOMOUS_TRANSACTION, confirmamos aquí:
    //await conn.commit();

    return { ok: true };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  } finally {
    if (conn)
      try {
        await conn.close();
      } catch {}
  }
}

module.exports = { ejecutarValidaCupon };
