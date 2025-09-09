const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function getDataCartola({ tipo, start, end }) {
  const connection = await getConnection();

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

async function getDataHistorial({ rut, tipo }) {
  const connection = await getConnection();
  const tabla_lcn = 'LCN_TBK_HISTORICO';
  const tabla_ldn = 'LDN_TBK_HISTORICO';
  let sql;

  switch (tipo) {
    case 'LCN':
      sql = `
        SELECT
            CASE
                WHEN a.liq_orpedi IS NOT NULL
                    AND a.liq_orpedi > 0
                    AND REGEXP_LIKE(a.liq_orpedi, '^[1-9][0-9]*$')
                THEN a.liq_orpedi
                ELSE a.liq_codaut
            END AS cupon_Credito,
            b.rut AS rut,
            a.liq_fcom AS fecha_venta,
            a.liq_fpago AS fecha_abono,
            a.liq_cuotas AS cuota_pagada,
            a.liq_ntc AS TOTAL_CUOTAS,
            TRUNC(a.liq_monto / 100) AS monto,
            vec_cob01.pip_obtiene_nombre(b.rut) AS nombre,
            NVL(a.liq_ntc, 0) - NVL(a.liq_cuotas, 0) AS cuotas_restantes,
            CASE WHEN NVL(a.liq_cuotas,0) > 0
                  THEN (a.liq_monto / 100) / a.liq_cuotas
                  ELSE a.liq_monto
              END AS deuda_pagada,
            (NVL(a.liq_ntc,0) - NVL(a.liq_cuotas,0)) * (NVL(a.liq_monto,0) / 100) AS deuda_por_pagar
        FROM
            ${tabla_lcn} a,
            proceso_cupon b
        WHERE
            (a.liq_orpedi = b.cupon OR a.liq_codaut = b.cupon)
            AND b.rut = :rut`;
      break;

    case 'LDN':
      sql = `
        SELECT
            CASE
                WHEN a.liq_nro_unico IS NOT NULL
                    AND a.liq_nro_unico > 0
                    AND REGEXP_LIKE(a.liq_nro_unico, '^[1-9][0-9]*$')
                THEN a.liq_nro_unico
                ELSE a.liq_appr
            END AS cupon_Credito, -- Alias estandarizado
            b.rut AS rut,
            vec_cob01.pip_obtiene_nombre(b.rut) AS nombre,
            a.liq_fcom AS fecha_venta,
            a.liq_fedi AS fecha_abono,
            TRUNC(a.liq_amt_1 / 100) AS monto,
            1 AS cuota_pagada,
            1 AS TOTAL_CUOTAS,
            0 AS cuotas_restantes,
            TRUNC(a.liq_amt_1 / 100) AS deuda_pagada,
            0 AS deuda_por_pagar
        FROM
            ${tabla_ldn} a,
            proceso_cupon b
        WHERE
            (a.liq_nro_unico = b.cupon OR a.liq_appr = b.cupon)
            AND b.rut = :rut`;
      break;

    default:
      throw new Error('Tipo de registro no valido');
  }

  let result;
  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { rut };
    result = await connection.execute(sql, binds, options);
    return result.rows || [];
  } catch (error) {
    console.error('Error al ejecutar la consulta:', error);
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
  getDataCartola,
  getTotalesWebpay,
  getDataHistorial,
};
