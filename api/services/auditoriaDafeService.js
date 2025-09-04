const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function enviarATesoreriaSoloSiSinPendientes({ usuarioId, observacion }) {
  const conn = await getConnection();
  try {
    // Ejemplo simple: contar aprobados por enviar (puedes dejar 0 si quieres ultra-minimal)
    const rAprob = await conn.execute(
      `SELECT COUNT(*) AS CANT
         FROM CUADRATURA_FILE_TBK
        WHERE STATUS_SAP_REGISTER = 'APROBADO'`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const cant = rAprob.rows[0].CANT ?? 0;

    await conn.execute(
      `INSERT INTO LOG_ENVIO_TESORERÍA (ID_AUD, USUARIO, REGISTROS_ENVIADOS)
       VALUES (SEQ_AUD_ENVIO_TESORERIA.NEXTVAL, :usuario, :cant)`,
      { usuario: usuarioId, cant }, // <-- SOLO binds usados
      { autoCommit: false }
    );

    await conn.commit();
    return { ok: true };
  } catch (e) {
    try {
      await conn.rollback();
    } catch {}
    throw e;
  } finally {
    try {
      await conn.close();
    } catch {}
  }
}

module.exports = { enviarATesoreriaSoloSiSinPendientes };
