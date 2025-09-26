function buildLiquidacionQuery({ tipo, startLCN, startLDN }) {
  const tipoUpper = (tipo || '').toUpperCase();

  const isValid = (col) => `REGEXP_LIKE(TRIM(l.${col}), '^[0-9]*[1-9][0-9]*$')`;

  let where = '';
  let binds = {};

  if (tipoUpper === 'LCN') {
    where = "l.TIPO_TRANSACCION = 'LCN' AND l.liq_fpago = :fecha";
    binds.fecha = startLCN;
  } else if (tipoUpper === 'LDN') {
    where = "l.TIPO_TRANSACCION = 'LDN' AND l.liq_fedi = :fecha";
    binds.fecha = startLDN;
  } else {
    throw new Error(`Tipo de transacción no soportado: ${tipo}`);
  }

  let joinClause = '';
  if (tipoUpper === 'LCN') {
    joinClause = `
      INNER JOIN CCN_TBK_HISTORICO h ON
        (
          ${isValid('liq_orpedi')} AND
          LTRIM(TRIM(l.liq_orpedi), '0') = h.DKTT_DT_NUMERO_UNICO
        ) OR
        (
          NOT ${isValid('liq_orpedi')} AND
          TRIM(l.liq_codaut) = h.DKTT_DT_APPRV_CDE -- <<< ¡AQUÍ ESTÁ LA CORRECCIÓN!
        )
    `;
  } else if (tipoUpper === 'LDN') {
    joinClause = `
      INNER JOIN CDN_TBK_HISTORICO h ON
        (
          ${isValid('liq_nro_unico')} AND
          TRIM(l.liq_nro_unico) = h.DSK_ID_NRO_UNICO
        ) OR
        (
          NOT ${isValid('liq_nro_unico')} AND
          TRIM(l.liq_appr) = h.DSK_APPVR_CDE
        )
    `;
  }

  // El resto de las expresiones de la función no necesitan cambios...
  const cuponExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN ${isValid('liq_orpedi')} THEN LTRIM(TRIM(l.liq_orpedi), '0') ELSE TRIM(l.liq_codaut) END`
      : `CASE WHEN ${isValid('liq_nro_unico')} THEN TRIM(l.liq_nro_unico) ELSE TRIM(l.liq_appr) END`;

  const codAutorizacionExpr = tipoUpper === 'LCN' ? 'TRIM(l.liq_codaut)' : 'TRIM(l.liq_appr)';

  const comercioExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN l.liq_cprin != 99999999 THEN TRIM(l.liq_cprin) ELSE TRIM(l.liq_numc) END`
      : 'TRIM(l.liq_numc)';

  const comisionExpr = `
    CASE
      WHEN l.TIPO_TRANSACCION = 'LCN' THEN (l.liq_monto / 100) * 0.0090
      WHEN l.TIPO_TRANSACCION = 'LDN' THEN (l.liq_monto / 100) * 0.0058
      ELSE 0
    END
  `;
  const montoBrutoExpr = `${comisionExpr} + (${comisionExpr} * 0.19)`;

  const fechaAbono = `
    CASE
      WHEN l.TIPO_TRANSACCION = 'LCN' THEN TO_CHAR(TO_DATE(l.liq_fpago, 'DDMMYYYY'), 'DD/MM/YYYY')
      WHEN l.TIPO_TRANSACCION = 'LDN' THEN TO_CHAR(TO_DATE(l.liq_fedi, 'DD/MM/YY'), 'DD/MM/YYYY')
      ELSE NULL
    END
  `;

  const fechaVenta = `TO_CHAR(TO_DATE(LPAD(TO_CHAR(l.liq_fcom), 8, '0'), 'DDMMYYYY'), 'DD/MM/YYYY')`;

  const orderBy = `
    ORDER BY
      CASE
        WHEN REGEXP_LIKE(TRIM(${cuponExpr}), '^[0-9]+$') THEN TO_NUMBER(TRIM(${cuponExpr}))
        ELSE NULL
      END NULLS LAST,
      l.date_load_bbdd DESC NULLS LAST
  `;

  const sql = `
    SELECT
      ${fechaAbono}             AS FECHA_ABONO,
      ${comercioExpr}           AS CODIGO_COMERCIO,
      ${cuponExpr}              AS CUPON,
      TRUNC(l.liq_monto / 100)  AS TOTAL_ABONADO,
      ${comisionExpr}           AS COMISION_NETA,
      ${montoBrutoExpr}         AS COMISION_BRUTA,
      ${fechaVenta}             AS FECHA_VENTA,
      CASE
        WHEN l.TIPO_TRANSACCION = 'LCN' THEN l.liq_cuotas
        ELSE 1
      END AS CUOTA,
      CASE
        WHEN l.TIPO_TRANSACCION = 'LCN' THEN l.liq_ntc
        ELSE 1
      END AS TOTAL_CUOTAS,
      ${codAutorizacionExpr}    AS CODIGO_AUTORIZACION
    FROM liquidacion_file_tbk l
    ${joinClause}
    WHERE
    ${where}
    ${orderBy}
  `;

  return { sql, binds };
}

function buildCartolaQuery({ tipo, start, end }) {
  const sql = `
    SELECT
      TO_NUMBER(TRIM(liq.liq_orpedi)) AS cupon,
      liq.liq_fcom AS fecha_venta,
      wt.orden_compra AS rut,
      TRUNC(liq.liq_monto/100) AS monto,
      liq.liq_cuotas AS cuota,
      liq.liq_ntc AS total_cuotas,
      NVL(liq.liq_ntc, 0) - NVL(liq.liq_cuotas, 0) AS cuotas_restantes,
      CASE WHEN NVL(liq.liq_cuotas,0) > 0
           THEN (liq.liq_monto / 100) / liq.liq_cuotas
           ELSE NULL
      END AS deuda_pagada,
      (NVL(liq.liq_ntc,0) - NVL(liq.liq_cuotas,0)) * (NVL(liq.liq_monto,0) / 100) AS deuda_por_pagar,
      liq.liq_fpago AS fecha_abono,
      liq.liq_nombre_banco AS nombre_banco
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
    WHERE liq.tipo_transaccion = :tipo
      AND TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY')
          BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
              AND TO_DATE(:fecha_fin, 'DDMMYYYY')
  `;

  const binds = { tipo, fecha_ini: start, fecha_fin: end };

  return { sql, binds };
}

module.exports = { buildLiquidacionQuery, buildCartolaQuery };
