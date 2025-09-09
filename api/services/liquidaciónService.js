const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function getLiquidacion({ tipo, start, end }) {
  const connection = await getConnection();

  // helper: numérico y no-solo-ceros
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  const tipoUpper = (tipo || '').toUpperCase();

  // Expresión CUPON por tipo
  let cuponExpr;
  if (tipoUpper === 'LCN') {
    cuponExpr = `
      CASE
        WHEN ${isValid('liq_orpedi')} THEN LTRIM(TRIM(liq_orpedi), '0')
        ELSE TRIM(liq_codaut)
      END
    `;
  } else if (tipoUpper === 'LDN') {
    cuponExpr = `
      CASE
        WHEN ${isValid('liq_nro_unico')} THEN TRIM(liq_nro_unico)
        ELSE TRIM(liq_appr)
      END
    `;
  } else {
    // Por si aparece otro tipo, usa un fallback neutro (puedes ajustar)
    cuponExpr = `TRIM(COALESCE(liq_nro_unico, liq_orpedi, liq_codaut, liq_appr))`;
  }

  // Lógica para el código de autorización
  let codAutorizacionExpr;
  if (tipoUpper === 'LCN') {
    codAutorizacionExpr = `TRIM(liq_codaut)`;
  } else if (tipoUpper === 'LDN') {
    codAutorizacionExpr = `TRIM(liq_appr)`;
  } else {
    codAutorizacionExpr = `TRIM(COALESCE(liq_codaut, liq_appr))`;
  }

  // Lógica para el código de comercio
  let comercioExpr;
  if (tipoUpper === 'LCN') {
    comercioExpr = `
      CASE
        WHEN liq_cprin != 99999999 THEN TRIM(liq_cprin)
        ELSE TRIM(liq_numc)
      END
    `;
  } else {
    // Para transacciones LDN y cualquier otro tipo, usa liq_numc
    comercioExpr = `TRIM(liq_numc)`;
  }

  //TODO: PARAMETRIZAR IVA Y COMISION
  // Lógica para la comisión y el monto bruto
  const comisionExpr = `
    CASE
      WHEN TIPO_TRANSACCION = 'LCN' THEN (liq_monto / 100) * 0.0090
      WHEN TIPO_TRANSACCION = 'LDN' THEN (liq_monto / 100) * 0.0058
      ELSE 0
    END
  `;

  const montoBrutoExpr = `
  ${comisionExpr} + (${comisionExpr} * 0.19)
`;

  // ORDER BY: intenta ordenar numéricamente el CUPON si lo es; si no, cae por fecha
  const orderBy = `
    ORDER BY
      CASE
        WHEN REGEXP_LIKE(TRIM(${cuponExpr}), '^[0-9]+$') THEN TO_NUMBER(TRIM(${cuponExpr}))
        ELSE NULL
      END NULLS LAST,
      date_load_bbdd DESC NULLS LAST
  `;

  const fechaAbono = `
    CASE 
      WHEN TIPO_TRANSACCION = 'LCN' THEN liq_fpago
      WHEN TIPO_TRANSACCION = 'LDN' THEN liq_fedi 
      ELSE NULL
    END
   `;

  const sql = `
    SELECT
      ${fechaAbono}                               AS FECHA_ABONO,
      ${comercioExpr}                           AS CODIGO_COMERCIO,
      ${cuponExpr}                            AS CUPON,
      TRUNC(liq_monto / 100)                  AS TOTAL_ABONADO,
      ${comisionExpr}                           AS COMISION_NETA,
      ${montoBrutoExpr}                         AS COMISION_BRUTA,
      liq_fcom                              AS FECHA_VENTA,
      CASE 
        WHEN TIPO_TRANSACCION = 'LCN' THEN liq_cuotas
        ELSE NULL
      END AS CUOTA,
      CASE 
        WHEN TIPO_TRANSACCION = 'LCN' THEN liq_ntc
        ELSE NULL
      END AS TOTAL_CUOTAS,
      ${codAutorizacionExpr}                   AS CODIGO_AUTORIZACION
    FROM liquidacion_file_tbk
    WHERE TIPO_TRANSACCION = :tipo
      AND DATE_LOAD_BBDD >= :startDate
      AND DATE_LOAD_BBDD <  :endDate
    ${orderBy}
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { tipo: tipoUpper, startDate: start, endDate: end };

    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('error', error);
    throw error;
  } finally {
    try {
      await connection.close();
    } catch (_) {}
  }
}

async function getLiquidacionTotales({ tipo, start, end }) {
  const connection = await getConnection();

  const tipoUpper = (tipo || '').toUpperCase();

  let comercioExpr;
  if (tipoUpper === 'LCN') {
    comercioExpr = `CASE WHEN l.liq_cprin != 99999999 THEN TRIM(l.liq_cprin) ELSE TRIM(l.liq_numc) END`;
  } else {
    comercioExpr = `TRIM(l.liq_numc)`;
  }

  const sql = `
    SELECT
      ${comercioExpr}                           AS CODIGO_COMERCIO,
      c.NOMBRE_COMERCIO AS NOMBRE_COMERCIO,
      SUM(l.liq_monto / 100)                     AS TOTAL_MONTO
    FROM liquidacion_file_tbk l
    LEFT JOIN vec_cob04.codigo_comerico c
    ON c.codigo_comerico = ${comercioExpr}
    WHERE TIPO_TRANSACCION = :tipo
    AND l.DATE_LOAD_BBDD >= :startDate
    AND l.DATE_LOAD_BBDD <  :endDate
    AND ${comercioExpr} NOT IN ('28208820', '48211418')
    GROUP BY ${comercioExpr}, c.NOMBRE_COMERCIO
    ORDER BY TOTAL_MONTO DESC
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { tipo: tipoUpper, startDate: start, endDate: end };
    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('Error al obtener totales de liquidación:', error);
    throw error;
  } finally {
    try {
      await connection.close();
    } catch (_) {}
  }
}

module.exports = {
  getLiquidacion,
  getLiquidacionTotales,
};
