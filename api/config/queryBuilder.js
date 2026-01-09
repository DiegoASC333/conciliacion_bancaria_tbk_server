function buildLiquidacionQuery({ tipo, startLCN, startLDN }) {
  const tipoUpper = (tipo || '').toUpperCase();
  const isValid = (col) => `REGEXP_LIKE(TRIM(l.${col}), '^[0-9]*[1-9][0-9]*$')`;

  const cuponExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN ${isValid('liq_orpedi')} THEN LTRIM(TRIM(l.liq_orpedi), '0') ELSE LTRIM(TRIM(l.liq_codaut), '0') END`
      : `CASE WHEN ${isValid('liq_nro_unico')} THEN LTRIM(TRIM(l.liq_nro_unico), '0') ELSE LTRIM(TRIM(l.liq_appr), '0') END`;

  const nombreColumnaFechaPC = 'FECHA';

  let where = '';
  let binds = {};
  let joinClause = '';
  let joinProcesoCupon = '';

  if (tipoUpper === 'LCN') {
    where = `l.TIPO_TRANSACCION = 'LCN' AND l.liq_fpago = :fecha`;
    binds.fecha = startLCN;

    const lcn_l_dateFormat = 'DDMMYYYY';
    const lcn_h_dateFormat = 'RRMMDD';

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
        AND REGEXP_LIKE(TO_CHAR(l.liq_fcom), '^[0-9]{7,8}$') 
        AND REGEXP_LIKE(TO_CHAR(h.DKTT_DT_TRAN_DAT), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(l.liq_fcom), 8, '0'), '${lcn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DKTT_DT_TRAN_DAT), 6, '0'), '${lcn_h_dateFormat}')
    `; //AND TRUNC(l.liq_monto/100) = h.DKTT_DT_AMT_1 Se quita de momento para incongruencia entre monto abonado y total de venta

    joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DKTT_DT_TRAN_DAT)
        AND pc.id_cuadratura = h.id_ccn
    `;
  } else if (tipoUpper === 'LDN') {
    where = `
      l.TIPO_TRANSACCION = 'LDN'
      AND REGEXP_LIKE(TRIM(l.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
      AND TO_DATE(TRIM(l.liq_fedi), 'DD/MM/RR') = TO_DATE(:fecha, 'DD/MM/RR')
    `;

    binds.fecha = startLDN;

    const ldn_l_dateFormat = 'DDMMRR';
    const ldn_h_dateFormat = 'RRMMDD';

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
        AND REGEXP_LIKE(TO_CHAR(l.liq_fcom), '^[0-9]{5,6}$')
        AND REGEXP_LIKE(TO_CHAR(h.DSK_TRAN_DAT), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(l.liq_fcom), 6, '0'), '${ldn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DSK_TRAN_DAT), 6, '0'), '${ldn_h_dateFormat}')
        AND l.liq_monto = h.DSK_AMT_1 
    `; //Se agrega monto para evitar problemas con cupones repetidos

    joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DSK_TRAN_DAT)
        AND pc.id_cuadratura = h.id_cdn
    `;
  }

  const codAutorizacionExpr = tipoUpper === 'LCN' ? 'TRIM(l.liq_codaut)' : 'TRIM(l.liq_appr)';

  const comercioExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN l.liq_cprin != 99999999 THEN l.liq_cprin ELSE l.liq_numc END`
      : `l.liq_numc`;

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

  // const orderBy = `
  //   ORDER BY
  //     CASE
  //       WHEN REGEXP_LIKE(TRIM(${cuponExpr}), '^[0-9]+$') THEN TO_NUMBER(TRIM(${cuponExpr}))
  //       ELSE NULL
  //     END NULLS LAST,
  //     l.date_load_bbdd DESC NULLS LAST
  // `;

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
      l.id as id,
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
      NVL(TO_CHAR(pc.rut), '-') as RUT,
      ${codAutorizacionExpr}    AS CODIGO_AUTORIZACION,
      NVL(h.tipo_documento, 'Z5') as TIPO_DOCUMENTO,
      ${esValidoExpr}           AS DAFE
    FROM liquidacion_file_tbk l
    ${joinClause}
    ${joinProcesoCupon}
    WHERE
    ${where}
    AND ${comercioExpr} NOT IN ('28208820', '48211418', '41246590', '41246593', '41246594')
    ${orderBy}
  `;

  return { sql, binds };
}

function buildCartolaQuery({ tipo, start, end }) {
  let sql;
  const binds = { fecha_ini: start, fecha_fin: end };
  let joinProcesoCupon = '';
  let joinWebpay = '';

  const nombreColumnaFechaPC = 'FECHA';

  const tipoUpper = (tipo || '').toUpperCase();
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  const cuponExpr =
    tipoUpper === 'LCN'
      ? `CASE WHEN ${isValid('liq.liq_orpedi')} THEN LTRIM(TRIM(liq.liq_orpedi), '0') ELSE TRIM(liq.liq_codaut) END`
      : `CASE WHEN ${isValid('liq.liq_nro_unico')} THEN LTRIM(TRIM(liq.liq_nro_unico), '0') ELSE LTRIM(TRIM(liq.liq_appr), '0') END`;

  if (tipo === 'LCN') {
    const lcn_l_dateFormat = 'DDMMYYYY';
    const lcn_h_dateFormat = 'RRMMDD';

    const fechaAbono = `TO_CHAR(TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY'), 'DD/MM/YYYY')`;

    const fechaVenta = `
      CASE
        WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$')
        THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY'), 'DD/MM/YYYY')
        ELSE NULL
      END
    `;

    joinWebpay = `LEFT JOIN (
         SELECT id_sesion, orden_compra
         FROM vec_cob02.webpay_trasaccion
         WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
       ) wt ON TO_NUMBER(TRIM(liq.liq_orpedi)) = TO_NUMBER(TRIM(wt.id_sesion))`;

    const joinDocumento = `
      LEFT JOIN CCN_TBK_HISTORICO h ON
        (
          ${isValid('liq.liq_orpedi')} AND
          LTRIM(TRIM(liq.liq_orpedi), '0') = LTRIM(TRIM(h.DKTT_DT_NUMERO_UNICO), '0')
        ) OR
        (
          NOT ${isValid('liq.liq_orpedi')} AND
          TRIM(liq.liq_codaut) = h.DKTT_DT_APPRV_CDE 
        )
        AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$')
        AND REGEXP_LIKE(TO_CHAR(h.DKTT_DT_TRAN_DAT), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), '${lcn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DKTT_DT_TRAN_DAT), 6, '0'), '${lcn_h_dateFormat}')
    `;

    joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DKTT_DT_TRAN_DAT)
        AND pc.id_cuadratura = h.id_ccn
    `;

    sql = `
      SELECT
        id_lcn as id,
        ${cuponExpr}              AS CUPON,
        liq.liq_codaut as CODIGO_AUTORIZACION,
        ${fechaVenta} AS FECHA_VENTA,
        
        -- AJUSTE: Siempre calcular el total de la venta original desde la tabla liq 
        -- para que no dependa de si el abono ya ocurrió o no.
        COALESCE(
          ROUND(h.DKTT_DT_AMT_1/100), 
          ROUND(liq.liq_monto/100) * liq.liq_ntc
        ) AS VENTA_TOTAL_ORIGINAL,
        
        COALESCE(TO_CHAR(pc.rut), TO_CHAR(wt.orden_compra)) AS RUT,

        -- Este es el abono del periodo (Cajita Verde)
        CASE
          WHEN TO_DATE(LPAD(TRIM(liq.liq_fpago), 8, '0'), 'DDMMYYYY') > TO_DATE(:fecha_fin, 'DDMMYYYY')
          THEN 0
          ELSE ROUND(liq.liq_monto/100)
        END AS MONTO,

        -- Agregamos el monto real de la cuota (sin filtro de fecha) para cálculos internos
        ROUND(liq.liq_monto/100) AS MONTO_CUOTA_REAL,

        liq.liq_cuotas AS CUOTA,          
        liq.liq_ntc AS TOTAL_CUOTAS,
        TRIM(liq.liq_rete) AS RETE,       
        ${fechaAbono} AS FECHA_ABONO,
        COALESCE(
          h.TIPO_DOCUMENTO,
          REGEXP_SUBSTR(JSON_VALUE(pc.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+'),
          'Z5'
        ) AS TIPO_DOCUMENTO,

        -- AJUSTE: La venta del periodo debe marcarse aunque el abono sea futuro
        CASE
          WHEN TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY')
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY')
              -- En lugar de liq.liq_cuotas = 1, podrías usar una lógica de "Mínima cuota encontrada"
              -- Pero por ahora, asegúrate que la fecha de compra (fcom) sea la que manda.
          THEN 1 ELSE 0
        END AS ES_VENTA_PERIODO,

        CASE
          WHEN TO_DATE(LPAD(TRIM(liq.liq_fpago), 8, '0'), 'DDMMYYYY')
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY')
          THEN 1 ELSE 0
        END AS ES_ABONO_PERIODO
      FROM LCN_TBK_HISTORICO liq
      ${joinWebpay}
      ${joinDocumento}
      ${joinProcesoCupon}
      WHERE REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
      AND REGEXP_LIKE(TRIM(liq.liq_fpago), '^[0-9]{7,8}$')        
      AND (
        (TO_DATE(LPAD(TRIM(liq.liq_fpago), 8, '0'), 'DDMMYYYY') BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY'))
        OR 
        (TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY') BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY'))
      )
    `;
  } else if (tipo === 'LDN') {
    const ldn_l_dateFormat = 'DDMMRR';
    const ldn_h_dateFormat = 'RRMMDD';

    const fechaAbono = `
      CASE
        WHEN REGEXP_LIKE(TRIM(liq.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
        THEN TO_CHAR(TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/RR'), 'DD/MM/YYYY')
        ELSE NULL
      END
    `;

    const fechaVenta = `
      CASE
        WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$')
        THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR'), 'DD/MM/YYYY')
        ELSE NULL
      END
    `;

    joinWebpay = `LEFT JOIN (
         SELECT id_sesion, orden_compra
         FROM vec_cob02.webpay_trasaccion
         WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
       ) wt ON TO_NUMBER(TRIM(liq.liq_nro_unico)) = TO_NUMBER(TRIM(wt.id_sesion))`;

    const joinDocumento = `
      LEFT JOIN CDN_TBK_HISTORICO h ON
        (
          ${isValid('liq.liq_nro_unico')} AND
          LTRIM(TRIM(liq.liq_nro_unico), '0') = LTRIM(TRIM(h.DSK_ID_NRO_UNICO), '0')
        ) OR
        (
          NOT ${isValid('liq.liq_nro_unico')} AND
          TRIM(liq.liq_appr) = h.DSK_APPVR_CDE
        )
         AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$')
        AND REGEXP_LIKE(TO_CHAR(h.DSK_TRAN_DAT), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), '${ldn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DSK_TRAN_DAT), 6, '0'), '${ldn_h_dateFormat}')
        AND liq.LIQ_AMT_1 = h.DSK_AMT_1 
    `;

    joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DSK_TRAN_DAT)
        AND pc.id_cuadratura = h.id_cdn
    `;

    sql = `
      SELECT
        id_ldn as id,
        ${cuponExpr}              AS CUPON,
        liq.liq_appr as CODIGO_AUTORIZACION,
        ${fechaVenta} AS FECHA_VENTA,
        COALESCE(TO_CHAR(pc.rut), TO_CHAR(wt.orden_compra)) AS RUT,
        TRUNC(liq.liq_amt_1/100) AS MONTO,
        1 AS CUOTA,
        1 AS TOTAL_CUOTAS,
        0 AS CUOTAS_REMANENTES,
        TRUNC(liq.liq_amt_1/100) AS DEUDA_PAGADA,
        0 AS DEUDA_POR_PAGAR,
        ${fechaAbono} AS FECHA_ABONO,
        COALESCE(
          h.TIPO_DOCUMENTO, -- 1. Prioridad
          REGEXP_SUBSTR(JSON_VALUE(pc.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+'), 
          'Z5'
        ) AS TIPO_DOCUMENTO,
        CASE
          WHEN REGEXP_LIKE(TRIM(liq.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$') AND 
               TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/RR') > TO_DATE(:fecha_fin, 'DDMMYYYY')
          THEN TRUNC(liq.liq_amt_1/100)
          ELSE 0
        END AS MONTO_PENDIENTE,
        -- INDICADOR 1: ¿Es una venta realizada en el periodo?
        CASE
          WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$') AND 
              TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR') 
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY')
          THEN 1 ELSE 0
        END AS ES_VENTA_PERIODO,
        -- INDICADOR 2: ¿Es un abono recibido en el periodo?
        CASE
          WHEN REGEXP_LIKE(TRIM(liq.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$') AND 
              TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/RR') 
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY')
          THEN 1 ELSE 0
        END AS ES_ABONO_PERIODO
      FROM LDN_TBK_HISTORICO liq 
      ${joinWebpay}
      ${joinDocumento}
      ${joinProcesoCupon}
      WHERE REGEXP_LIKE(TRIM(liq.liq_nro_unico), '^\\d+$')
      AND (
        -- Escenario A: Movimientos que se ABONARON en el rango
        (
          REGEXP_LIKE(TRIM(liq.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$') 
          AND TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/RR') 
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY')
        )
        OR 
        -- Escenario B: Movimientos que se VENDIERON en el rango (pero quizás se abonan después)
        (
          REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$') 
          AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR') 
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY') AND TO_DATE(:fecha_fin, 'DDMMYYYY')
        )
      )
      `;

    // AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR')
    //         BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
    //             AND TO_DATE(:fecha_fin, 'DDMMYYYY')
  }

  return { sql, binds };
}

function buildVentasQuery({ tipo, start, end }) {
  let sql;
  const tipoUpper = (tipo || '').toUpperCase();

  // Mantenemos tu validador de columnas
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  // 1. Definimos cuponExpr usando alias de tabla 't' para evitar ambigüedad con el JOIN
  const cuponExpr =
    tipoUpper === 'CCN'
      ? `CASE 
        WHEN ${isValid('t.dktt_dt_numero_unico')} THEN LTRIM(TRIM(t.dktt_dt_numero_unico), '0') 
        ELSE LTRIM(TRIM(t.dktt_dt_apprv_cde), '0') 
       END`
      : `CASE 
        WHEN ${isValid('t.dsk_id_nro_unico')} THEN LTRIM(TRIM(t.dsk_id_nro_unico), '0') 
        ELSE LTRIM(TRIM(t.dsk_appvr_cde), '0') 
       END`;

  // 2. Definimos el JOIN (el RUT viene de la tabla pc)
  const joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}`;

  const formatDateForOracle = (dateStr) => {
    if (!dateStr) return null;
    return dateStr.replace(/-/g, '').substring(2);
  };

  const startOracle = formatDateForOracle(start);
  const endOracle = formatDateForOracle(end);

  const binds = {
    fecha_ini: startOracle,
    fecha_fin: endOracle,
  };

  if (tipoUpper === 'CCN') {
    sql = `SELECT 
        t.id_ccn as id,
        t.dktt_dt_tran_dat as fecha_venta, 
        (t.dktt_dt_amt_1/100) as monto_venta, 
        CASE 
          WHEN t.dktt_dt_canti_cuotas IS NULL  THEN 1
          ELSE t.dktt_dt_canti_cuotas 
        END as cuotas, 
        t.dktt_dt_apprv_cde as codigo_autorizacion, 
        ${cuponExpr} as cupon,
        t.tipo_documento,
        pc.RUT as rut
    FROM 
        ccn_tbk_historico t
    ${joinProcesoCupon}
    WHERE 
        t.dktt_dt_tran_dat BETWEEN :fecha_ini AND :fecha_fin`;
  } else if (tipoUpper === 'CDN') {
    sql = `SELECT 
          t.id_cdn as id,
          t.dsk_tran_dat as fecha_venta, 
          (t.dsk_amt_1/100) as monto_venta, 
          t.dsk_appvr_cde as codigo_autorizacion,
          ${cuponExpr} as cupon,
          t.tipo_documento,
          pc.RUT as rut 
      FROM 
          cdn_tbk_historico t
      ${joinProcesoCupon}
      WHERE t.dsk_tran_dat BETWEEN :fecha_ini AND :fecha_fin`;
  }

  return { sql, binds };
}

module.exports = { buildLiquidacionQuery, buildCartolaQuery, buildVentasQuery };
