const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

function buildSaldoPendienteQuery({ fecha }) {
  const binds = { fecha_fin: fecha };
  const nombreColumnaFechaPC = 'FECHA';
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  // Expresión para determinar el CUPON
  const cuponExpr = `CASE WHEN ${isValid('liq.liq_orpedi')} THEN LTRIM(TRIM(liq.liq_orpedi), '0') ELSE TRIM(liq.liq_codaut) END`;

  // Formatos de fecha para los JOINs
  const lcn_l_dateFormat = 'DDMMYYYY';
  const lcn_h_dateFormat = 'RRMMDD';

  // Expresiones de fecha y monto
  const fechaAbono = `TO_CHAR(TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY'), 'DD/MM/YYYY')`;
  const fechaVenta = `
    CASE
      WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$')
      THEN TO_CHAR(TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY'), 'DD/MM/YYYY')
      ELSE NULL
    END
  `;
  const cutOffDate = '01102025';

  const montoTotalVenta = `
  CASE
    WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$') AND
         TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY') < TO_DATE('${cutOffDate}', 'DDMMYYYY')
    -- Calcular como el MONTO TOTAL: valor cuota * total_cuotas
    THEN ROUND(liq.liq_monto / 100) * liq.liq_ntc 
    -- Si es posterior o igual, usar el monto de la tabla de documentos
    ELSE ROUND(h.DKTT_DT_AMT_1/100)
  END
`;

  // JOINs
  const joinWebpay = `LEFT JOIN (
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

  const joinProcesoCupon = `
    LEFT JOIN PROCESO_CUPON pc 
      ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
      AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DKTT_DT_TRAN_DAT)
      AND pc.id_cuadratura = h.id_ccn
  `;

  const sql = `
    SELECT
      ${cuponExpr} AS CUPON,
      liq.liq_codaut AS CODIGO_AUTORIZACION,
      ${fechaVenta} AS fecha_venta,
      NVL(ROUND(h.DKTT_DT_AMT_1/100), (liq.liq_monto / 100) * liq.liq_ntc) AS monto_total_venta,
      ROUND(liq.liq_monto/100) AS monto, -- Monto de la cuota abonada
      liq.liq_cuotas AS cuota,          
      liq.liq_ntc AS total_cuotas,
      TRIM(liq.liq_rete) AS RETE,       
      ${fechaAbono} AS fecha_abono,
      COALESCE(
        h.TIPO_DOCUMENTO,
        REGEXP_SUBSTR(JSON_VALUE(pc.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+'),
        'Z5'
      ) AS TIPO_DOCUMENTO
    FROM LCN_TBK_HISTORICO liq
    ${joinWebpay}
    ${joinDocumento}
    ${joinProcesoCupon}
    WHERE REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
     AND REGEXP_LIKE(TRIM(liq.liq_fcom), '^[0-9]{8}$') 
      -- FILTRO DE FECHA ÚNICA (HASTA la fecha)
      AND TO_DATE(TRIM(liq.liq_fcom), 'DDMMYYYY')
          <= TO_DATE(:fecha_fin, 'DDMMYYYY')
    ORDER BY liq.liq_fproc, liq.liq_orpedi, liq.liq_cuotas
  `;

  return { sql, binds };
}

async function getSaldoPendienteLCN({ fecha }) {
  const connection = await getConnection();

  try {
    const { sql, binds } = buildSaldoPendienteQuery({ fecha });

    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sql, binds, options);

    // Agrupación y procesamiento de las filas
    const filasProcesadas = (res.rows || []).map((r) => {
      const monto = r.MONTO;
      let cuota = r.CUOTA;
      let totalCuotas = r.TOTAL_CUOTAS;
      const monto_total_venta = r.MONTO_TOTAL_VENTA;

      const isLcnFullPayment = (r.RETE || '').trim() === '0000';

      // Ajuste para pagos totales o sin cuotas
      if (isLcnFullPayment || !totalCuotas || totalCuotas === 0) {
        cuota = 1;
        totalCuotas = 1;
      }

      let cuotasRestantes, deudaPagada, deudaPorPagar, montoVenta;

      // Cálculo de métricas de deuda (Agrupación/Procesamiento)
      if (totalCuotas === 1) {
        // Caso pago total o transacción simple
        cuota = 1;
        totalCuotas = 1;
        cuotasRestantes = 0;
        deudaPagada = monto;
        montoVenta = monto_total_venta;
        deudaPorPagar = 0;
      } else {
        // Caso cuotas
        cuotasRestantes = totalCuotas - cuota;
        deudaPagada = cuota * monto;
        montoVenta = monto_total_venta;
        deudaPorPagar = montoVenta - deudaPagada;

        // Ajuste de redondeo (tolerancia de 1 unidad)
        if (Math.abs(deudaPorPagar) <= 1) {
          deudaPorPagar = 0;
        }
      }

      return {
        ID: r.ID,
        CUPON: r.CUPON,
        CODIGO_AUTORIZACIÓN: r.CODIGO_AUTORIZACION,
        FECHA_VENTA: r.FECHA_VENTA,
        RUT: r.RUT,
        MONTO: monto, // Monto de la cuota abonada
        CUOTA: cuota,
        TOTAL_CUOTAS: totalCuotas,
        CUOTAS_RESTANTES: cuotasRestantes,
        MONTO_VENTA: montoVenta, // Monto total original de la venta
        DEUDA_PAGADA: deudaPagada, // Monto acumulado pagado hasta esta cuota
        DEUDA_POR_PAGAR: deudaPorPagar, // Saldo pendiente
        FECHA_ABONO: r.FECHA_ABONO,
        TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
      };
    });

    // Opcional: Calcular totales generales (suma de saldos pendientes, etc.)
    const totalDeudaPendiente = filasProcesadas.reduce((sum, row) => sum + row.DEUDA_POR_PAGAR, 0);

    // 2. Calcular totales agrupados por TIPO_DOCUMENTO
    const totalesPorDocumento = filasProcesadas.reduce((acc, row) => {
      const tipo = row.TIPO_DOCUMENTO;

      // Inicializar el grupo si no existe
      if (!acc[tipo]) {
        acc[tipo] = {
          total_transacciones: 0,
          total_deuda_pendiente: 0,
        };
      }

      // Acumular
      acc[tipo].total_transacciones += 1;
      acc[tipo].total_deuda_pendiente += row.DEUDA_POR_PAGAR;

      return acc;
    }, {});

    return {
      detalle_transacciones: filasProcesadas,
      totales: {
        total_transacciones: filasProcesadas.length,
        total_deuda_pendiente: totalDeudaPendiente,
        totales_por_documento: totalesPorDocumento,
      },
    };
  } catch (error) {
    console.error('Error en getSaldoPendienteLCN:', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

module.exports = {
  getSaldoPendienteLCN,
};
