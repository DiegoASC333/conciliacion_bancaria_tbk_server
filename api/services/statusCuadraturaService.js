const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function getStatusDiarioCuadratura({ start, end }) {
  const connection = await getConnection();

  const sql = ` SELECT
    COUNT(*) AS TOTAL_DIARIO,
    SUM(TRUNC(DKTT_DT_AMT_1/100)) AS MONTO_TOTAL_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN 1 ELSE 0 END) AS APROBADOS_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_APROBADOS,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN 1 ELSE 0 END) AS RECHAZADOS_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_RECHAZADOS,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESO','RE-PROCESADO') THEN 1 ELSE 0 END) AS REPROCESADOS_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESO','RE-PROCESADO') THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_REPROCESADOS
    FROM CUADRATURA_FILE_TBK
    WHERE DATE_LOAD_BBDD >= :startDate
    AND DATE_LOAD_BBDD <  :endDate`;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { startDate: start, endDate: end }; // JS Date -> bind DATE/TIMESTAMP

    const res = await connection.execute(sql, binds, options);
    const r = res.rows?.[0] || {};

    return {
      total_diario: Number(r.TOTAL_DIARIO || 0),
      monto_total_diario: Number(r.MONTO_TOTAL_DIARIO || 0),
      aprobados_diario: Number(r.APROBADOS_DIARIO || 0),
      monto_aprobados: Number(r.MONTO_APROBADOS || 0),
      rechazados_diario: Number(r.RECHAZADOS_DIARIO || 0),
      monto_rechazados: Number(r.MONTO_RECHAZADOS || 0),
      reprocesados_diario: Number(r.REPROCESADOS_DIARIO || 0),
      monto_reprocesados: Number(r.MONTO_REPROCESADOS || 0),
    };
  } catch (error) {
    console.error('error', error);
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

// En tu servicio
async function listarPorTipo({ estados, validarCupon = true }) {
  const conn = await getConnection();
  try {
    const binds = {};
    const conditions = [];

    // 1) Filtro por estado (si corresponde)
    if (Array.isArray(estados) && estados.length > 0) {
      const bindNames = estados.map((_, i) => `:estado_${i}`).join(', ');
      estados.forEach((v, i) => (binds[`estado_${i}`] = String(v).toUpperCase()));
      conditions.push(`UPPER(NVL(STATUS_SAP_REGISTER, '')) IN (${bindNames})`);
    }

    // Helper para validar que es numérico y no solo ceros
    const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

    // 2) Filtro de cupón válido (solo para aprobados/reprocesados/total)
    if (validarCupon) {
      conditions.push(
        `( ${isValid('DKTT_DT_NUMERO_UNICO')} OR ${isValid('DSK_ID_NRO_UNICO')} OR TRIM(DKTT_DT_APPRV_CDE) IS NOT NULL )`
      );
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    // 3) Lógica CUPON único con fallback
    const cuponExpr = `
      CASE
        WHEN ${isValid('DSK_ID_NRO_UNICO')}     THEN LTRIM(TRIM(DSK_ID_NRO_UNICO), '0')
        WHEN ${isValid('DKTT_DT_NUMERO_UNICO')} THEN LTRIM(TRIM(DKTT_DT_NUMERO_UNICO), '0')
        WHEN TRIM(DKTT_DT_APPRV_CDE) IS NOT NULL THEN TRIM(DKTT_DT_APPRV_CDE)
        ELSE NULL
      END
    `;

    const sql = `
      SELECT
        ${cuponExpr}                       AS CUPON,
        TRUNC(DKTT_DT_AMT_1/100)           AS MONTO_TRANSACCION,
        DKTT_TIPO_CUOTA                    AS TIPO_CUOTA,
        DKTT_DT_CANTI_CUOTAS               AS CUOTAS,
        TO_CHAR(TO_DATE(TO_CHAR(DKTT_DT_FECHA_PAGO, 'FM000000'), 'RRMMDD'), 'YYYYMMDD') AS FECHA_ABONO,
        TO_CHAR(TO_DATE(TO_CHAR(DKTT_DT_TRAN_DAT, 'FM000000'), 'RRMMDD'), 'YYYYMMDD')  AS FECHA_VENTA,
        CASE
          WHEN tipo_transaccion = 'CCN' THEN 'Crédito'
          WHEN tipo_transaccion = 'CDN' THEN 'Débito'
          ELSE tipo_transaccion
        END AS TIPO_TRANSACCION
      FROM cuadratura_file_tbk
      ${whereClause}
       ORDER BY 
        CASE 
          WHEN REGEXP_LIKE(${cuponExpr}, '^[0-9]+$') THEN 1 ELSE 2 
        END, -- primero los numéricos
        CASE 
          WHEN REGEXP_LIKE(${cuponExpr}, '^[0-9]+$') THEN TO_NUMBER(${cuponExpr})
        END, -- orden numérico real
        ${cuponExpr}, -- luego orden alfabético para los alfanuméricos
        date_load_bbdd DESC NULLS LAST
    `;

    const res = await conn.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return res.rows || [];
  } finally {
    try {
      await conn.close();
    } catch {}
  }
}

module.exports = {
  getStatusDiarioCuadratura,
  listarPorTipo,
};
