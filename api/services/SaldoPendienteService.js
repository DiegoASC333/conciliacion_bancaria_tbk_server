const { getConnection, exportToExcel } = require('../config/utils');
const oracledb = require('oracledb');

function buildSaldoPendienteQuery({ fecha, tipo }) {
  const tipoUpper = (tipo || '').toUpperCase();
  let sql = '';
  const binds = { fecha_fin: fecha };
  const nombreColumnaFechaPC = 'FECHA';
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  if (tipoUpper === 'LCN') {
    const cuponExpr = `CASE WHEN ${isValid('liq.liq_orpedi')} THEN LTRIM(TRIM(liq.liq_orpedi), '0') ELSE TRIM(liq.liq_codaut) END`;
    const lcn_l_dateFormat = 'DDMMYYYY';
    const lcn_h_dateFormat = 'RRMMDD';

    const fechaAbono = `TO_CHAR(TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY'), 'DD/MM/YYYY')`;
    const fechaVenta = `
      CASE
        WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$')
        THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY'), 'DD/MM/YYYY')
        ELSE NULL
      END`;

    const joinWebpay = `LEFT JOIN (
     SELECT id_sesion, orden_compra
     FROM vec_cob02.webpay_trasaccion
     WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
    ) wt ON TO_NUMBER(TRIM(liq.liq_orpedi)) = TO_NUMBER(TRIM(wt.id_sesion))`;

    const joinDocumento = `
      LEFT JOIN CCN_TBK_HISTORICO h ON
        ((${isValid('liq.liq_orpedi')} AND LTRIM(TRIM(liq.liq_orpedi), '0') = LTRIM(TRIM(h.DKTT_DT_NUMERO_UNICO), '0')) 
        OR (NOT ${isValid('liq.liq_orpedi')} AND TRIM(liq.liq_codaut) = h.DKTT_DT_APPRV_CDE))
        AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$')
        AND REGEXP_LIKE(TO_CHAR(h.DKTT_DT_TRAN_DAT), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), '${lcn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DKTT_DT_TRAN_DAT), 6, '0'), '${lcn_h_dateFormat}')`;

    const joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DKTT_DT_TRAN_DAT)
        AND pc.id_cuadratura = h.id_ccn`;

    sql = `
    SELECT
      ${cuponExpr} AS CUPON,
      liq.liq_codaut AS CODIGO_AUTORIZACION,
      ${fechaVenta} AS fecha_venta,
      NVL(ROUND(h.DKTT_DT_AMT_1/100), (liq.liq_monto / 100) * liq.liq_ntc) AS monto_total_venta,
      ROUND(liq.liq_monto/100) AS monto,
      liq.liq_cuotas AS CUOTA,          
      liq.liq_ntc AS TOTAL_CUOTAS,
      TRIM(liq.liq_rete) AS RETE,       
      ${fechaAbono} AS fecha_abono,
      COALESCE(h.TIPO_DOCUMENTO, REGEXP_SUBSTR(JSON_VALUE(pc.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+'), 'Z5') AS TIPO_DOCUMENTO
    FROM LCN_TBK_HISTORICO liq
    ${joinWebpay}
    ${joinDocumento}
    ${joinProcesoCupon}
    WHERE REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
      AND REGEXP_LIKE(TRIM(liq.liq_fcom), '^[0-9]{8}$') 
      AND TO_DATE(TRIM(liq.liq_fcom), 'DDMMYYYY') <= TO_DATE(:fecha_fin, 'DDMMYYYY')
      AND (liq.liq_fpago IS NULL OR TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY') > TO_DATE(:fecha_fin, 'DDMMYYYY'))
    ORDER BY liq.liq_fproc, liq.liq_orpedi, liq.liq_cuotas`;
  } else if (tipoUpper === 'LDN') {
    const cuponExpr = `CASE WHEN ${isValid('liq.liq_nro_unico')} THEN LTRIM(TRIM(liq.liq_nro_unico), '0') ELSE LTRIM(TRIM(liq.liq_appr), '0') END`;
    const ldn_l_dateFormat = 'DDMMRR';
    const ldn_h_dateFormat = 'RRMMDD';

    const fechaAbono = `
      CASE
        WHEN REGEXP_LIKE(TRIM(liq.liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
        THEN TO_CHAR(TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/RR'), 'DD/MM/YYYY')
        ELSE NULL
      END`;

    const fechaVenta = `
      CASE
        WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$')
        THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR'), 'DD/MM/YYYY')
        ELSE NULL
      END`;

    const joinWebpay = `LEFT JOIN (
         SELECT id_sesion, orden_compra
         FROM vec_cob02.webpay_trasaccion
         WHERE REGEXP_LIKE(TRIM(id_sesion), '^\\d+$')
       ) wt ON TO_NUMBER(TRIM(liq.liq_nro_unico)) = TO_NUMBER(TRIM(wt.id_sesion))`;

    const joinDocumento = `
      LEFT JOIN CDN_TBK_HISTORICO h ON
        ((${isValid('liq.liq_nro_unico')} AND LTRIM(TRIM(liq.liq_nro_unico), '0') = LTRIM(TRIM(h.DSK_ID_NRO_UNICO), '0'))
        OR (NOT ${isValid('liq.liq_nro_unico')} AND TRIM(liq.liq_appr) = h.DSK_APPVR_CDE))
        AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$')
        AND REGEXP_LIKE(TO_CHAR(h.DSK_TRAN_DAT), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), '${ldn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DSK_TRAN_DAT), 6, '0'), '${ldn_h_dateFormat}')
        AND liq.LIQ_AMT_1 = h.DSK_AMT_1`;

    const joinProcesoCupon = `
      LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DSK_TRAN_DAT)
        AND pc.id_cuadratura = h.id_cdn`;

    sql = `
      SELECT
        ${cuponExpr} AS CUPON,
        liq.liq_appr AS CODIGO_AUTORIZACION,
        ${fechaVenta} AS fecha_venta,
        TRUNC(liq.liq_amt_1/100) AS monto_total_venta,
        TRUNC(liq.liq_amt_1/100) AS monto,
        ${fechaAbono} AS fecha_abono,
        COALESCE(h.TIPO_DOCUMENTO, REGEXP_SUBSTR(JSON_VALUE(pc.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+'), 'Z5') AS TIPO_DOCUMENTO,
        TRIM(liq.liq_fedi) as raw_fecha_abono
      FROM LDN_TBK_HISTORICO liq 
      ${joinWebpay}
      ${joinDocumento}
      ${joinProcesoCupon}
      WHERE REGEXP_LIKE(TRIM(liq.liq_nro_unico), '^\\d+$')
      AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$')
      AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR') <= TO_DATE(:fecha_fin, 'DDMMYYYY')
      AND (liq.liq_fedi IS NULL OR TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/RR') > TO_DATE(:fecha_fin, 'DDMMYYYY'))
      ORDER BY liq.liq_fpro , liq.liq_nro_unico`;
  }

  return { sql, binds };
}

async function getSaldoPendienteService({ fecha, tipo }) {
  const connection = await getConnection();
  const tipoUpper = (tipo || '').toUpperCase();

  try {
    const { sql, binds } = buildSaldoPendienteQuery({ fecha, tipo: tipoUpper });
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sql, binds, options);

    // 1. Convertir la fecha de consulta (DDMMYYYY) a objeto Date para comparar
    const dia = parseInt(fecha.substring(0, 2));
    const mes = parseInt(fecha.substring(2, 4)) - 1; // Meses en JS son 0-11
    const anio = parseInt(fecha.substring(4, 8));
    const fechaCorte = new Date(anio, mes, dia, 23, 59, 59, 999);

    const filasProcesadas = (res.rows || []).map((r) => {
      const montoCuota = r.MONTO;
      const cuota = parseInt(r.CUOTA) || 1;
      const totalCuotas = parseInt(r.TOTAL_CUOTAS) || 1;
      const montoTotalVenta = r.MONTO_TOTAL_VENTA;

      let deudaPorPagar = 0;

      if (tipoUpper === 'LDN') {
        let esPendiente = false;

        if (!r.RAW_FECHA_ABONO) {
          // Caso A: No hay fecha de abono aún, es deuda pendiente
          esPendiente = true;
        } else {
          // Caso B: Hay fecha de abono, comparamos con el corte
          const partes = r.RAW_FECHA_ABONO.split('/');
          let dA = parseInt(partes[0]),
            mA = parseInt(partes[1]) - 1,
            aA = parseInt(partes[2]);
          if (aA < 100) aA += 2000;

          const fechaAbono = new Date(aA, mA, dA, 0, 0, 0, 0);

          // Si el abono ocurrió DESPUÉS de la fecha consultada, es saldo pendiente a esa fecha
          if (fechaAbono.getTime() > fechaCorte.getTime()) {
            esPendiente = true;
          }
        }

        deudaPorPagar = esPendiente ? montoTotalVenta : 0;
      } else {
        // LCN (Crédito) - Lógica de Flujo de Caja por Cuota
        let esCuotaPendiente = false;

        if (!r.FECHA_ABONO) {
          esCuotaPendiente = true;
        } else {
          const partes = r.FECHA_ABONO.split('/');
          const dA = parseInt(partes[0]),
            mA = parseInt(partes[1]) - 1,
            aA = parseInt(partes[2]);
          const fechaAbonoCuota = new Date(aA, mA, dA, 0, 0, 0, 0);

          if (fechaAbonoCuota.getTime() > fechaCorte.getTime()) {
            esCuotaPendiente = true;
          }
        }
        // Si la cuota específica está fuera de fecha, la deuda es el monto de ESA cuota
        deudaPorPagar = esCuotaPendiente ? montoCuota : 0;
      }

      return {
        CUPON: r.CUPON,
        CODIGO_AUTORIZACION: r.CODIGO_AUTORIZACION,
        FECHA_VENTA: r.FECHA_VENTA,
        FECHA_ABONO: r.FECHA_ABONO, // Para visualizar cuándo se pagó realmente
        MONTO_VENTA: montoTotalVenta,
        CUOTA: cuota,
        TOTAL_CUOTAS: totalCuotas,
        DEUDA_POR_PAGAR: deudaPorPagar,
        TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
      };
    });

    const totalDeudaPendiente = filasProcesadas.reduce((sum, row) => sum + row.DEUDA_POR_PAGAR, 0);

    return {
      detalle_transacciones: filasProcesadas,
      totales: {
        total_transacciones: filasProcesadas.length,
        total_deuda_pendiente: totalDeudaPendiente,
      },
    };
  } finally {
    if (connection) await connection.close();
  }
}

async function getSaldoPendienteServiceExcel({ fecha, tipo }, res) {
  let connection;
  const tipoUpper = (tipo || '').toUpperCase();

  try {
    connection = await getConnection();
    const { sql, binds } = buildSaldoPendienteQuery({ fecha, tipo: tipoUpper });

    const result = await connection.execute(sql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    const dia = parseInt(fecha.substring(0, 2));
    const mes = parseInt(fecha.substring(2, 4)) - 1;
    const anio = parseInt(fecha.substring(4, 8));
    const fechaCorte = new Date(anio, mes, dia, 23, 59, 59, 999);

    const datosProcesados = result.rows.map((r) => {
      const montoCuota = r.MONTO || 0;
      const montoTotalVenta = r.MONTO_TOTAL_VENTA || 0;
      const rawAbono = (r.RAW_FECHA_ABONO || r.FECHA_ABONO || '').trim();

      let deudaPorPagar = 0;
      let esPendiente = false;

      if (!rawAbono) {
        esPendiente = true;
      } else {
        let dA, mA, aA;
        if (rawAbono.includes('/')) {
          const partes = rawAbono.split('/');
          dA = parseInt(partes[0]);
          mA = parseInt(partes[1]) - 1;
          aA = parseInt(partes[2]);
          if (aA < 100) aA += 2000;
        } else {
          dA = parseInt(rawAbono.substring(0, 2));
          mA = parseInt(rawAbono.substring(2, 4)) - 1;
          aA = parseInt(rawAbono.substring(4, 8));
        }

        const fechaAbono = new Date(aA, mA, dA, 0, 0, 0, 0);
        if (fechaAbono.getTime() > fechaCorte.getTime()) esPendiente = true;
      }

      deudaPorPagar = esPendiente ? (tipoUpper === 'LCN' ? montoCuota : montoTotalVenta) : 0;

      // Retornamos un objeto limpio para el Excel
      return {
        CUPON: r.CUPON,
        'COD. AUTORIZACIÓN': r.CODIGO_AUTORIZACION,
        'TIPO DOC': r.TIPO_DOCUMENTO,
        'FECHA VENTA': r.FECHA_VENTA,
        'FECHA ABONO': r.FECHA_ABONO,
        'MONTO VENTA': montoTotalVenta,
        CUOTA: parseInt(r.CUOTA) || 1,
        'TOTAL CUOTAS': parseInt(r.TOTAL_CUOTAS) || 1,
        'SALDO PENDIENTE': deudaPorPagar,
      };
    });

    // Filtramos para que el Excel solo tenga las deudas reales
    const soloPendientes = datosProcesados.filter((f) => f['SALDO PENDIENTE'] > 0);

    // Llamamos a tu función de exportación
    const nombreArchivo = `Saldo_Pendiente_${tipoUpper}_${fecha}.xlsx`;
    await exportToExcel(soloPendientes, res, nombreArchivo);
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

module.exports = {
  getSaldoPendienteService,
  getSaldoPendienteServiceExcel,
};
