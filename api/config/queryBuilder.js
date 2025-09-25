function buildLiquidacionQuery({ tipo, startLCN, startLDN }) {
  const tipoUpper = (tipo || '').toUpperCase();

  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  let where = '';
  let binds = {};

  if (tipoUpper === 'LCN') {
    where = "TIPO_TRANSACCION = 'LCN' AND liq_fpago = :fecha";
    binds.fecha = startLCN;
  } else if (tipoUpper === 'LDN') {
    where = "TIPO_TRANSACCION = 'LDN' AND liq_fedi = :fecha";
    binds.fecha = startLDN;
  } else {
    throw new Error(`Tipo de transacción no soportado: ${tipo}`);
  }

  // CUPON
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
    cuponExpr = `TRIM(COALESCE(liq_nro_unico, liq_orpedi, liq_codaut, liq_appr))`;
  }

  // Cód Autorización
  let codAutorizacionExpr =
    tipoUpper === 'LCN'
      ? 'TRIM(liq_codaut)'
      : tipoUpper === 'LDN'
        ? 'TRIM(liq_appr)'
        : 'TRIM(COALESCE(liq_codaut, liq_appr))';

  // Comercio
  let comercioExpr =
    tipoUpper === 'LCN'
      ? `
        CASE
          WHEN liq_cprin != 99999999 THEN TRIM(liq_cprin)
          ELSE TRIM(liq_numc)
        END
      `
      : 'TRIM(liq_numc)';

  // Comisión y bruto
  const comisionExpr = `
    CASE
      WHEN TIPO_TRANSACCION = 'LCN' THEN (liq_monto / 100) * 0.0090
      WHEN TIPO_TRANSACCION = 'LDN' THEN (liq_monto / 100) * 0.0058
      ELSE 0
    END
  `;

  const montoBrutoExpr = `${comisionExpr} + (${comisionExpr} * 0.19)`;

  // Fecha Abono
  const fechaAbono = `
    CASE 
      WHEN TIPO_TRANSACCION = 'LCN' THEN TO_CHAR(TO_DATE(liq_fpago, 'DDMMYYYY'), 'DD/MM/YYYY')
      WHEN TIPO_TRANSACCION = 'LDN' THEN TO_CHAR(TO_DATE(liq_fedi, 'DD/MM/YY'), 'DD/MM/YYYY')
      ELSE NULL
    END
  `;

  // Fecha Venta
  // const fechaVenta = `
  //   CASE
  //     WHEN TIPO_TRANSACCION = 'LCN' THEN TO_CHAR(TO_DATE(liq_fcom, 'DDMMYYYY'), 'DD/MM/YYYY')
  //     WHEN TIPO_TRANSACCION = 'LDN' THEN TO_CHAR(TO_DATE(liq_fcom, 'DD/MM/YY'), 'DD/MM/YYYY')
  //     ELSE NULL
  //   END
  // `;

  const fechaVenta = `
  TO_CHAR( -- 3. Finalmente, da el formato de salida que quieres 'DD/MM/YYYY'
    TO_DATE( -- 2. Convierte la cadena de 8 dígitos a una fecha real
      LPAD(TO_CHAR(liq_fcom), 8, '0'), -- 1. Transforma el NÚMERO a una CADENA de 8 dígitos, rellenando con '0' a la izquierda si es necesario
      'DDMMYYYY'
    ),
    'DD/MM/YYYY'
  )
`;

  const orderBy = `
    ORDER BY
      CASE
        WHEN REGEXP_LIKE(TRIM(${cuponExpr}), '^[0-9]+$') THEN TO_NUMBER(TRIM(${cuponExpr}))
        ELSE NULL
      END NULLS LAST,
      date_load_bbdd DESC NULLS LAST
  `;

  // SQL final
  const sql = `
    SELECT
      ${fechaAbono}             AS FECHA_ABONO,
      ${comercioExpr}           AS CODIGO_COMERCIO,
      ${cuponExpr}              AS CUPON,
      TRUNC(liq_monto / 100)    AS TOTAL_ABONADO,
      ${comisionExpr}           AS COMISION_NETA,
      ${montoBrutoExpr}         AS COMISION_BRUTA,
      ${fechaVenta}             AS FECHA_VENTA,
      CASE 
        WHEN TIPO_TRANSACCION = 'LCN' THEN liq_cuotas
        ELSE 1
      END AS CUOTA,
      CASE 
        WHEN TIPO_TRANSACCION = 'LCN' THEN liq_ntc
        ELSE 1
      END AS TOTAL_CUOTAS,
      ${codAutorizacionExpr}    AS CODIGO_AUTORIZACION
    FROM liquidacion_file_tbk
    WHERE
    ${where}
    ${orderBy}
  `;

  //const binds = { tipo: tipoUpper, startLCN, startLDN };
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
