function buildLiquidacionQuery({ tipo, startLCN, startLDN }) {
  const tipoUpper = (tipo || '').toUpperCase();

  const isValid = (col) => `REGEXP_LIKE(TRIM(l.${col}), '^[0-9]*[1-9][0-9]*$')`;

  let where = '';
  let binds = {};

  if (tipoUpper === 'LCN') {
    where = "l.TIPO_TRANSACCION = 'LCN' AND l.liq_fpago = :fecha";
    binds.fecha = startLCN;
  } else if (tipoUpper === 'LDN') {
    where = `
      l.TIPO_TRANSACCION = 'LDN'
      AND REGEXP_LIKE(TRIM(l.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
      AND TO_DATE(TRIM(l.liq_fedi), 'DD/MM/RR') = TO_DATE(:fecha, 'DD/MM/RR')
    `;

    binds.fecha = startLDN;
  } else {
    throw new Error(`Tipo de transacción no soportado: ${tipo}`);
  }

  let joinClause = '';
  if (tipoUpper === 'LCN') {
    joinClause = `
      LEFT JOIN CCN_TBK_HISTORICO h ON
        (
          ${isValid('liq_orpedi')} AND
          LTRIM(TRIM(l.liq_orpedi), '0') = LTRIM(TRIM(h.DKTT_DT_NUMERO_UNICO), '0')
        ) OR
        (
          NOT ${isValid('liq_orpedi')} AND
          TRIM(l.liq_codaut) = h.DKTT_DT_APPRV_CDE 
        )
    `;
  } else if (tipoUpper === 'LDN') {
    joinClause = `
      LEFT JOIN CDN_TBK_HISTORICO h ON
        (
          ${isValid('liq_nro_unico')} AND
          LTRIM(TRIM(l.liq_nro_unico), '0') = LTRIM(TRIM(h.DSK_ID_NRO_UNICO), '0')
        ) OR
        (
          NOT ${isValid('liq_nro_unico')} AND
          TRIM(l.liq_appr) = h.DSK_APPVR_CDE
        )
    `;
  }

  const cuponExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN ${isValid('liq_orpedi')} THEN LTRIM(TRIM(l.liq_orpedi), '0') ELSE TRIM(l.liq_codaut) END`
      : `CASE WHEN ${isValid('liq_nro_unico')} THEN LTRIM(TRIM(l.liq_nro_unico), '0') ELSE LTRIM(TRIM(l.liq_appr), '0') END`;

  const joinProcesoCupon = ` LEFT JOIN PROCESO_CUPON pc ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}`;

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
      WHEN l.TIPO_TRANSACCION = 'LCN'
        THEN TO_CHAR(TO_DATE(TRIM(l.liq_fpago), 'DDMMYYYY'), 'DD/MM/YYYY')
      WHEN l.TIPO_TRANSACCION = 'LDN'
        AND REGEXP_LIKE(TRIM(l.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
        THEN TO_CHAR(TO_DATE(TRIM(l.liq_fedi), 'DD/MM/RR'), 'DD/MM/YYYY')
      ELSE NULL
    END
  `;

  const fechaVenta = `
  CASE
    WHEN l.TIPO_TRANSACCION = 'LCN'
      AND REGEXP_LIKE(TO_CHAR(l.liq_fcom), '^[0-9]{7,8}$')
      THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(l.liq_fcom), 8, '0'), 'DDMMYYYY'), 'DD/MM/YYYY')
    WHEN l.TIPO_TRANSACCION = 'LDN'
      AND REGEXP_LIKE(TO_CHAR(l.liq_fcom), '^[0-9]{5,6}$')
      THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(l.liq_fcom), 6, '0'), 'DDMMRR'), 'DD/MM/YYYY')
    ELSE NULL
  END
`;

  const orderBy = `
    ORDER BY
      CASE
        WHEN REGEXP_LIKE(TRIM(${cuponExpr}), '^[0-9]+$') THEN TO_NUMBER(TRIM(${cuponExpr}))
        ELSE NULL
      END NULLS LAST,
      l.date_load_bbdd DESC NULLS LAST
  `;

  // === Indicador de validez (SI / NO) ===
  const esValidoExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN h.DKTT_DT_NUMERO_UNICO IS NOT NULL THEN 'SI' ELSE 'NO' END`
      : `CASE WHEN h.DSK_ID_NRO_UNICO IS NOT NULL THEN 'SI' ELSE 'NO' END`;

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
      ${codAutorizacionExpr}    AS CODIGO_AUTORIZACION,
      NVL(h.tipo_documento, 'Z5') as TIPO_DOCUMENTO,
      ${esValidoExpr}           AS DAFE
    FROM liquidacion_file_tbk l
    ${joinClause}
    ${joinProcesoCupon}
    WHERE
    ${where}
    ${orderBy}
  `;

  return { sql, binds };
}

function buildCartolaQuery({ tipo, start, end }) {
  let sql;
  const binds = { fecha_ini: start, fecha_fin: end };

  if (tipo === 'LCN') {
    sql = `
      SELECT
        TO_NUMBER(TRIM(liq.liq_orpedi)) AS cupon,
        liq.liq_fcom AS fecha_venta,
        wt.orden_compra AS rut,
        TRUNC(liq.liq_monto/100) AS monto,
        liq.liq_cuotas AS cuota,          
        liq.liq_ntc AS total_cuotas,      
        liq.liq_fpago AS fecha_abono,
        liq.liq_nombre_banco AS nombre_banco,
        REGEXP_SUBSTR(JSON_VALUE(p.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+') AS TIPO_DOCUMENTO ---CAMBIO AQUI DS
      FROM LCN_TBK_HISTORICO liq
      JOIN (
        SELECT id_sesion, orden_compra
        FROM vec_cob02.webpay_trasaccion
        WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
      ) wt
        ON TO_NUMBER(TRIM(liq.liq_orpedi)) = TO_NUMBER(TRIM(wt.id_sesion))
      JOIN  proceso_cupon p ON TO_NUMBER(TRIM(liq.liq_orpedi)) = p.cupon --CAMBIO AQUI DS 
      WHERE REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
        AND REGEXP_LIKE(TRIM(liq.liq_fpago), '^\\d{8}$')
        AND TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY')
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;
  } else if (tipo === 'LDN') {
    sql = `
      SELECT
        liq.liq_nro_unico AS cupon, -- <<< NUEVA COLUMNA
        liq.liq_fcom AS fecha_venta,
        wt.orden_compra AS rut, -- <<< Se mantiene JOIN para obtener el RUT
        TRUNC(liq.liq_amt_1/100) AS monto, -- <<< NUEVA COLUMNA
        -- Lógica de cuotas no aplica para LDN, se colocan valores fijos
        1 AS cuota,
        1 AS total_cuotas,
        0 AS cuotas_restantes,
        TRUNC(liq.liq_amt_1/100) AS deuda_pagada,
        0 AS deuda_por_pagar,
        liq.liq_fedi AS fecha_abono, -- <<< NUEVA COLUMNA
        NULL AS nombre_banco -- No se especificó banco para LDN
      FROM LDN_TBK_HISTORICO liq -- <<< CAMBIO DE TABLA
      JOIN (
        SELECT *
        FROM vec_cob02.webpay_trasaccion
        WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
      ) wt
        ON TO_NUMBER(TRIM(liq.liq_nro_unico)) = TO_NUMBER(TRIM(wt.id_sesion)) -- <<< IMPORTANTE: Se asume que liq_orpedi existe en LDN para el JOIN
      LEFT JOIN proceso_cupon p ON TO_NUMBER(TRIM(liq.liq_nro_unico)) = TO_NUMBER(TRIM(p.id_cuadratura))
      WHERE REGEXP_LIKE(TRIM(liq.liq_nro_unico), '^\\d+$')
        AND TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/YY') -- <<< NUEVA COLUMNA Y FORMATO
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;
  }

  return { sql, binds };
}

module.exports = { buildLiquidacionQuery, buildCartolaQuery };
