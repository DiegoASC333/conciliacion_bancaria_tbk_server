const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function getDataCartola({ tipo, start, end }) {
  const connection = await getConnection();

  // const montoPorCuota = `
  //   CASE WHEN NVL(liq.liq_ntc, 0) > 0
  //       THEN (liq.liq_monto / 100) / liq.liq_ntc
  //       ELSE 0
  //   END
  // `;

  // Espera body: { tipo: 'LCN', start: '06012025', end: '07012025' }  // DDMMYYYY
  const sql = `
    SELECT
      TO_NUMBER(TRIM(liq.liq_orpedi))              AS cupon,
      liq.liq_fcom                                 AS fecha_venta,
      wt.orden_compra                              AS rut,
      TRUNC(liq.liq_monto/100)                                AS monto,
      liq.liq_cuotas                               AS cuota,
      liq.liq_ntc                                  AS total_cuotas,
      NVL(liq.liq_ntc, 0) - NVL(liq.liq_cuotas, 0) AS cuotas_restantes,
      CASE WHEN NVL(liq.liq_cuotas,0) > 0
           THEN (liq.liq_monto / 100) / liq.liq_cuotas
           ELSE NULL
      END                                          AS deuda_pagada,
      (NVL(liq.liq_ntc,0) - NVL(liq.liq_cuotas,0)) * (NVL(liq.liq_monto,0) / 100) AS deuda_por_pagar,
      liq.liq_fpago                                AS fecha_abono,
      liq.liq_nombre_banco                         AS nombre_banco
    FROM (
      SELECT *
      FROM vec_cob04.liquidacion_file_tbk
      WHERE REGEXP_LIKE(TRIM(liq_orpedi), '^\\d+$')              -- cupon numérico
        AND REGEXP_LIKE(TRIM(liq_fpago), '^\\d{8}$')             -- fecha DDMMYYYY
    ) liq
    JOIN (
      SELECT *
      FROM vec_cob02.webpay_trasaccion
      WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')               -- id_sesion numérico
    ) wt
      -- Opción A: comparar como NÚMERO en ambos lados (seguro porque ya filtramos ambos)
      ON TO_NUMBER(TRIM(liq.liq_orpedi)) = TO_NUMBER(TRIM(wt.id_sesion))
      -- Opción B alternativa: comparar como TEXTO
      -- ON TRIM(liq.liq_orpedi) = TO_CHAR(wt.id_sesion)
    WHERE
      liq.tipo_transaccion = :tipo
      AND TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY')
          BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
              AND TO_DATE(:fecha_fin, 'DDMMYYYY')
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    // IMPORTANTE: los nombres del objeto deben coincidir con los placeholders del SQL
    const binds = {
      tipo,
      fecha_ini: start, // '06012025'
      fecha_fin: end, // '07012025'
    };

    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('error', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

const getTotalesWebpay = async ({ tipo, start, end }) => {
  const connection = await getConnection();
  const sql = `
    SELECT
      SUM(liq.liq_monto / 100) AS saldo_estimado,
      SUM(
        (NVL(liq.liq_ntc,0) - NVL(liq.liq_cuotas,0)) * (NVL(liq.liq_monto,0) / 100)
      ) AS saldo_por_cobrar
    FROM (
      SELECT *
      FROM vec_cob04.liquidacion_file_tbk
      WHERE REGEXP_LIKE(TRIM(liq_orpedi), '^\\d+$')
        AND REGEXP_LIKE(TRIM(liq_fpago), '^\\d{8}$')
    ) liq
    JOIN (
      SELECT *
      FROM vec_cob02.webpay_trasaccion
      WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
    ) wt
      ON TO_NUMBER(TRIM(liq.liq_orpedi)) = TO_NUMBER(TRIM(wt.id_sesion))
    WHERE
      liq.tipo_transaccion = :tipo
      AND TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY')
          BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
              AND TO_DATE(:fecha_fin, 'DDMMYYYY')
  `;
  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    // IMPORTANTE: los nombres del objeto deben coincidir con los placeholders del SQL
    const binds = {
      tipo,
      fecha_ini: start, // '06012025'
      fecha_fin: end, // '07012025'
    };

    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('error', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
};

module.exports = {
  getDataCartola,
  getTotalesWebpay,
};
