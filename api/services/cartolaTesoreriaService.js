const { getConnection, exportToExcel } = require('../config/utils');
const { buildCartolaQuery } = require('../config/queryBuilder');
const oracledb = require('oracledb');

async function getCartolaDataAndTotals({ tipo, start, end }) {
  const detalle = await getDataCartola({ tipo, start, end });
  const totales = await calcularTotalesEnNode(detalle, tipo);

  return { detalle, totales };
}

function calcularTotalesEnNode(detalle, tipo) {
  const resultado = {
    saldo_estimado: 0, // Cajita Verde
    saldo_por_cobrar: 0, // Cajita Amarilla
    saldo_total_ventas: 0, // Cajita Azul
  };

  if (!detalle || detalle.length === 0) return [resultado];

  // --- ESCENARIO DÉBITO (LDN): Resta Simple ---
  if (tipo === 'LDN') {
    detalle.forEach((r) => {
      if (r.ES_VENTA_PERIODO === 1) {
        resultado.saldo_total_ventas += Number(r.MONTO || 0);
      }
      if (r.ES_ABONO_PERIODO === 1) {
        resultado.saldo_estimado += Number(r.MONTO || 0);
      }
    });
    // Para Débito mantenemos tu lógica de resta
    resultado.saldo_por_cobrar = Math.max(
      0,
      resultado.saldo_total_ventas - resultado.saldo_estimado
    );
  } // --- ESCENARIO CRÉDITO (LCN): Deduplicación y Suma de Deuda ---
  else {
    const cuponesProcesadosVenta = new Set();
    const cuponesProcesadosDeuda = new Set();

    // Ordenamos: Cupón y luego Cuota descendente
    const detalleOrdenado = [...detalle].sort((a, b) => {
      if (a.CUPON < b.CUPON) return -1;
      if (a.CUPON > b.CUPON) return 1;
      return (Number(b.CUOTA) || 0) - (Number(a.CUOTA) || 0);
    });

    detalleOrdenado.forEach((r) => {
      const montoAbono = Number(r.MONTO || 0);
      const deudaFila = Number(r.DEUDA_POR_PAGAR || 0);
      const ventaOriginal = Number(r.VENTA_TOTAL_ORIGINAL || 0);

      // Abonos: Se suman siempre
      if (r.ES_ABONO_PERIODO === 1) {
        resultado.saldo_estimado += montoAbono;
      }

      // Deuda: Solo la cuota más alta por cada cupón
      if (!cuponesProcesadosDeuda.has(r.CUPON)) {
        resultado.saldo_por_cobrar += deudaFila;
        cuponesProcesadosDeuda.add(r.CUPON);
      }

      // Ventas: Solo una vez por cupón
      if (r.ES_VENTA_PERIODO === 1 && !cuponesProcesadosVenta.has(r.CUPON)) {
        resultado.saldo_total_ventas += ventaOriginal;
        cuponesProcesadosVenta.add(r.CUPON);
      }
    });
  }

  return [resultado];
}

async function getDataCartola({ tipo, start, end }) {
  const connection = await getConnection();

  try {
    const { sql, binds } = buildCartolaQuery({ tipo, start, end });
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sql, binds, options);

    const rows = res.rows || [];

    const filas = rows.map((r) => {
      // --- ESCENARIO 1: DÉBITO (LDN) ---
      if (tipo === 'LDN') {
        return {
          ID: r.ID,
          CUPON: r.CUPON,
          CODIGO_AUTORIZACION: r.CODIGO_AUTORIZACION,
          FECHA_VENTA: r.FECHA_VENTA,
          RUT: r.RUT,
          MONTO: r.MONTO,
          CUOTA: 1,
          TOTAL_CUOTAS: 1,
          CUOTAS_RESTANTES: 0,
          MONTO_VENTA: r.MONTO,
          DEUDA_PAGADA: r.ES_ABONO_PERIODO === 1 ? r.MONTO : 0,
          // Muestra el monto pendiente si la fecha de abono es futura al periodo
          DEUDA_POR_PAGAR:
            r.MONTO_PENDIENTE !== undefined ? r.MONTO_PENDIENTE : r.monto_pendiente || 0,
          FECHA_ABONO: r.FECHA_ABONO,
          TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
          ES_VENTA_PERIODO: r.ES_VENTA_PERIODO,
          ES_ABONO_PERIODO: r.ES_ABONO_PERIODO,
        };
      }

      // --- ESCENARIO 2: CRÉDITO (LCN) ---
      // Definimos variables base para LCN
      const montoCuotaAbonada = r.MONTO; // Puede ser 0 si es abono futuro
      const montoVentaOriginal = r.VENTA_TOTAL_ORIGINAL;
      //const esVentaPeriodo = r.ES_VENTA_PERIODO;

      // Lógica de cuotas (considerando pagos full/contado como 1 cuota)
      const isLcnFullPayment = (r.RETE || '').trim() === '0000';
      const cuotaActual = isLcnFullPayment ? 1 : r.CUOTA;
      const totalCuotas = isLcnFullPayment ? 1 : r.TOTAL_CUOTAS;

      // Deuda por pagar visual: Si es la venta nueva (cuota 1), muestra lo que falta cobrar
      let deudaCalculada = 0;
      if (totalCuotas > 0) {
        // Estimación lógica: Total venta menos lo que ya se debería haber pagado a la fecha
        deudaCalculada = montoVentaOriginal - cuotaActual * (montoVentaOriginal / totalCuotas);
      }
      // Retornamos el objeto para LCN
      return {
        ID: r.ID,
        CUPON: r.CUPON,
        CODIGO_AUTORIZACION: r.CODIGO_AUTORIZACION,
        FECHA_VENTA: r.FECHA_VENTA,
        RUT: r.RUT,
        MONTO: montoCuotaAbonada, // Lo que se abona hoy (valor cuota)
        CUOTA: cuotaActual,
        TOTAL_CUOTAS: totalCuotas,
        CUOTAS_RESTANTES: totalCuotas - cuotaActual,
        MONTO_VENTA: montoVentaOriginal, // Se mantiene visible para el usuario siempre
        DEUDA_PAGADA: r.ES_ABONO_PERIODO === 1 ? montoCuotaAbonada : 0,
        DEUDA_POR_PAGAR: Math.max(0, Math.round(deudaCalculada)),
        FECHA_ABONO: r.FECHA_ABONO,
        TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
        // Indicadores invisibles para la función calcularTotalesEnNode
        ES_VENTA_PERIODO: r.ES_VENTA_PERIODO,
        ES_ABONO_PERIODO: r.ES_ABONO_PERIODO,
        VENTA_TOTAL_ORIGINAL: montoVentaOriginal,
      };
    });

    return filas;
  } catch (error) {
    console.error('Error en getDataCartola:', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

const getTotalesWebpay = async ({ tipo, start, end }) => {
  const connection = await getConnection();
  const tipoUpper = (tipo || '').toUpperCase();
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  let sqlVentas;
  let sqlSaldos;

  const resultado = {
    saldo_estimado: 0,
    saldo_por_cobrar: 0,
    saldo_total_ventas: 0,
  };

  if (tipo === 'LCN') {
    const lcn_l_dateFormat = 'DDMMYYYY';
    const lcn_h_dateFormat = 'RRMMDD';
    const nombreColumnaFechaPC = 'FECHA';
    const cutOffDate = '01102025'; // Fecha de corte fija

    const montoTotalVentaCondicional = `
      CASE
        WHEN REGEXP_LIKE(TO_CHAR(liq.LIQ_FCOM), '^[0-9]{7,8}$') AND
             TO_DATE(LPAD(TO_CHAR(liq.LIQ_FCOM), 8, '0'), 'DDMMYYYY') < TO_DATE('${cutOffDate}', 'DDMMYYYY')
        THEN ROUND(liq.LIQ_MONTO / 100) * (liq.LIQ_NTC - liq.LIQ_CUOTAS)
        ELSE ROUND(h.DKTT_DT_AMT_1 / 100)
      END
    `;

    const cuponExpr =
      tipoUpper === 'LCN'
        ? `CASE WHEN ${isValid('liq.liq_orpedi')} THEN LTRIM(TRIM(liq.liq_orpedi), '0') ELSE TRIM(liq.liq_codaut) END`
        : `CASE WHEN ${isValid('liq.liq_nro_unico')} THEN LTRIM(TRIM(liq.liq_nro_unico), '0') ELSE LTRIM(TRIM(liq.liq_appr), '0') END`;

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
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), '${lcn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DKTT_DT_TRAN_DAT), 6, '0'), '${lcn_h_dateFormat}')`;

    const joinProcesoCupon = `
        LEFT JOIN PROCESO_CUPON pc 
        ON LTRIM(TRIM(pc.CUPON), '0') = ${cuponExpr}
        AND TO_CHAR(pc.${nombreColumnaFechaPC}) = TO_CHAR(h.DKTT_DT_TRAN_DAT)
        AND pc.id_cuadratura = h.id_ccn`;

    const montoTotalVenta = `
      CASE
        -- 2. Si la transacción es antigua (antes del cutOffDate), usar la lógica Crédito: (Monto por cuota * Total de cuotas)
        WHEN REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$') AND
             TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY') < TO_DATE('${cutOffDate}', 'DDMMYYYY')
        THEN ROUND (liq.liq_monto / 100) * liq.liq_ntc
        
        -- 3. Si es posterior o igual, usar el monto de la tabla de documentos
        ELSE ROUND(h.DKTT_DT_AMT_1/100)
      END
    `;

    sqlVentas = `
      SELECT DISTINCT
        SUM(h.DKTT_DT_AMT_1 / 100) AS saldo_total_ventas
      FROM LCN_TBK_HISTORICO liq 
      ${joinDocumento}
      
      WHERE
        REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
        
        -- FILTRO REVERTIDO: Solo por liq_fcom (Fecha de Venta)
        AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{7,8}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 8, '0'), 'DDMMYYYY')
        BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
        AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;

    //--${joinProcesoCupon}

    sqlSaldos = `
      SELECT
        SUM(liq.LIQ_MONTO / 100) AS saldo_estimado,
        
        SUM((NVL(liq.LIQ_NTC, 0) - NVL(liq.LIQ_CUOTAS, 0)) * (NVL(liq.LIQ_MONTO, 0)) / 100)AS saldo_por_cobrar
        
      FROM LCN_TBK_HISTORICO liq 
      WHERE
        REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
        AND REGEXP_LIKE(TO_CHAR(liq.liq_fpago), '^[0-9]{8}$') -- Validar el formato DDMMYYYY
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fpago), 8, '0'), 'DDMMYYYY')
        BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
        AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;

    // --- LÓGICA LDN (DÉBITO) ---
  } else if (tipo === 'LDN') {
    const ldn_l_dateFormat = 'DDMMRR';
    const ldn_h_dateFormat = 'RRMMDD';

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

    sqlVentas = `
      SELECT
        SUM(h.DSK_AMT_1 / 100) AS saldo_total_ventas
      FROM LDN_TBK_HISTORICO liq 
      ${joinDocumento}
       WHERE REGEXP_LIKE(TRIM(liq.liq_nro_unico), '^\\d+$')
        AND REGEXP_LIKE(TO_CHAR(liq.liq_fcom), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.liq_fcom), 6, '0'), 'DDMMRR')
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;

    sqlSaldos = `
      SELECT
        /* Saldo estimado: Suma de los montos de débito liquidados */
        SUM(liq.LIQ_AMT_1 / 100) AS saldo_estimado,
        0 AS saldo_por_cobrar
      FROM LDN_TBK_HISTORICO liq 
      ${joinDocumento}
      WHERE
        REGEXP_LIKE(liq.LIQ_FEDI, '^\\d{2}/\\d{2}/\\d{2}$') -- Validar el formato DD/MM/RR
        AND TO_DATE(liq.LIQ_FEDI, 'DD/MM/RR')
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;
  } else {
    return [resultado];
  }

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = {
      fecha_ini: start,
      fecha_fin: end,
    };

    //const res = await connection.execute(sql, binds, options);
    // Asegurar que devuelva 0 si SUM da NULL (ninguna fila encontrada)
    // if (res.rows.length > 0 && res.rows[0].saldo_estimado !== null) {
    //   return res.rows;
    // }

    const resVentas = await connection.execute(sqlVentas, binds, options);
    if (resVentas.rows.length > 0) {
      resultado.saldo_total_ventas = resVentas.rows[0].SALDO_TOTAL_VENTAS || 0;
    }

    const resSaldos = await connection.execute(sqlSaldos, binds, options);
    if (resSaldos.rows.length > 0) {
      resultado.saldo_estimado = resSaldos.rows[0].SALDO_ESTIMADO || 0;
      resultado.saldo_por_cobrar = resSaldos.rows[0].SALDO_POR_COBRAR || 0;
    }

    if (tipo === 'LDN') {
      const totalVentas = resultado.saldo_total_ventas;
      const totalAbonado = resultado.saldo_estimado;
      resultado.saldo_por_cobrar = Math.max(0, totalVentas - totalAbonado);
    } else {
      resultado.saldo_por_cobrar = resSaldos.rows[0].SALDO_POR_COBRAR || 0;
    }

    return [resultado];
    //return [{ saldo_estimado: 0, saldo_por_cobrar: 0 }];
  } catch (error) {
    console.error('error en getTotalesWebpay', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
};

const getTotalesWebpayPorDocumento = async ({ tipo, start, end }) => {
  const connection = await getConnection();
  let sql;

  // Helpers
  const isValid = (col) => `REGEXP_LIKE(TRIM(liq.${col}), '^[0-9]*[1-9][0-9]*$')`;

  // DEFINICIÓN BASE DEL TIPO DE DOCUMENTO
  // Si el join falla, será 'Z5'. Si encuentra el doc, usará ese.
  const baseTipoDoc = `NVL(h.tipo_documento, 'Z5')`;

  try {
    // --- LÓGICA LCN (CRÉDITO) ---
    if (tipo === 'LCN') {
      // Etiqueta específica para LCN
      const labelExpr = `${baseTipoDoc} || ' - Crédito'`;

      const joinClause = `
        LEFT JOIN CCN_TBK_HISTORICO h ON 
          ((${isValid('LIQ_ORPEDI')} AND 
          LTRIM(TRIM(liq.LIQ_ORPEDI), '0') = LTRIM(TRIM(h.DKTT_DT_NUMERO_UNICO), '0')) 
          OR (NOT ${isValid('LIQ_ORPEDI')} AND TRIM(liq.LIQ_CODAUT) = h.DKTT_DT_APPRV_CDE))
      `;

      const whereClause = `
        WHERE
          REGEXP_LIKE(TO_CHAR(liq.LIQ_FPAGO), '^[0-9]{8}$') 
          AND TO_DATE(TRIM(liq.LIQ_FPAGO), 'DDMMYYYY')
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                  AND TO_DATE(:fecha_fin, 'DDMMYYYY')
      `;

      sql = `
        WITH AllPayments AS (
            SELECT
                liq.liq_orpedi, liq.LIQ_MONTO, liq.LIQ_NTC, liq.LIQ_CUOTAS,
                NVL(h.TIPO_DOCUMENTO, 'Z5') AS BASE_TIPO_DOCUMENTO,
                -- Cálculo de la deuda pendiente
                (NVL(liq.LIQ_NTC, 0) - NVL(liq.LIQ_CUOTAS, 0)) * (NVL(liq.LIQ_MONTO, 0) / 100) AS DEUDA_PENDIENTE,
                -- Identificar la última cuota pagada dentro del periodo para evitar el sobreconteo
                ROW_NUMBER() OVER (PARTITION BY liq.liq_orpedi ORDER BY TO_DATE(TRIM(liq.LIQ_FPAGO), 'DDMMYYYY') DESC, liq.LIQ_CUOTAS DESC) as rn
            FROM LCN_TBK_HISTORICO liq
            ${joinClause}
            WHERE
              -- FILTRO DE FECHA CAMBIADO A LIQ_FPAGO
              REGEXP_LIKE(TO_CHAR(liq.LIQ_FPAGO), '^[0-9]{8}$')
              AND TO_DATE(TRIM(liq.LIQ_FPAGO), 'DDMMYYYY')
                  BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                      AND TO_DATE(:fecha_fin, 'DDMMYYYY')
        )
        SELECT
          t1.BASE_TIPO_DOCUMENTO || ' - Crédito' AS TIPO_DOCUMENTO,
          -- saldo_estimado: Suma correctamente todos los abonos en el periodo por documento
          SUM(t1.LIQ_MONTO / 100) AS saldo_estimado,
          -- saldo_por_cobrar: Suma la DEUDA_PENDIENTE SOLO de la última cuota (rn=1)
          SUM(CASE WHEN t1.rn = 1 THEN t1.DEUDA_PENDIENTE ELSE 0 END) AS saldo_por_cobrar
        FROM AllPayments t1
        GROUP BY t1.BASE_TIPO_DOCUMENTO
        ORDER BY saldo_estimado DESC
      `;

      // sql = `
      //   SELECT
      //     ${labelExpr}             AS TIPO_DOCUMENTO,
      //     SUM(liq.LIQ_MONTO / 100) AS saldo_estimado,
      //     SUM(
      //       (NVL(liq.LIQ_NTC, 0) - NVL(liq.LIQ_CUOTAS, 0)) * (NVL(liq.LIQ_MONTO, 0) / 100)
      //     )                        AS saldo_por_cobrar
      //   FROM LCN_TBK_HISTORICO liq
      //   ${joinClause}
      //   ${whereClause}
      //   GROUP BY ${baseTipoDoc}
      //   ORDER BY saldo_estimado DESC
      // `;
      // Nota: Group By usa la baseTipoDoc porque el sufijo es constante para toda la query.

      // --- LÓGICA LDN (DÉBITO) ---
    } else if (tipo === 'LDN') {
      const ldn_l_dateFormat = 'DDMMRR';
      const ldn_h_dateFormat = 'RRMMDD';

      // Etiqueta específica para LDN
      const labelExpr = `${baseTipoDoc} || ' - Débito'`;

      const joinClause = `
        LEFT JOIN CDN_TBK_HISTORICO h ON 
          ((${isValid('LIQ_NRO_UNICO')} AND LTRIM(TRIM(liq.LIQ_NRO_UNICO), '0') = LTRIM(TRIM(h.DSK_ID_NRO_UNICO), '0')) 
          OR (NOT ${isValid('LIQ_NRO_UNICO')} AND TRIM(liq.LIQ_APPR) = h.DSK_APPVR_CDE))
            AND REGEXP_LIKE(TO_CHAR(h.DSK_TRAN_DAT), '^[0-9]{5,6}$')
            AND TO_DATE(LPAD(TO_CHAR(liq.LIQ_FCOM), 6, '0'), '${ldn_l_dateFormat}') = TO_DATE(LPAD(TO_CHAR(h.DSK_TRAN_DAT), 6, '0'), '${ldn_h_dateFormat}')
            AND liq.LIQ_AMT_1 = h.DSK_AMT_1
      `;

      const whereClause = `
        WHERE
          REGEXP_LIKE(TO_CHAR(liq.LIQ_FCOM), '^[0-9]{5,6}$')
          AND TO_DATE(LPAD(TO_CHAR(liq.LIQ_FCOM), 6, '0'), 'DDMMRR')
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                  AND TO_DATE(:fecha_fin, 'DDMMYYYY')
      `;

      sql = `
        SELECT
          ${labelExpr}             AS TIPO_DOCUMENTO,
          SUM(liq.LIQ_AMT_1 / 100) AS saldo_estimado,
          0                        AS saldo_por_cobrar
        FROM LDN_TBK_HISTORICO liq
        ${joinClause}
        ${whereClause}
        GROUP BY ${baseTipoDoc}
        ORDER BY saldo_estimado DESC
      `;
    } else {
      return [];
    }

    // EJECUCIÓN
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { fecha_ini: start, fecha_fin: end };

    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('Error en getTotalesWebpayPorDocumento', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
};

async function getCartolaExcel({ tipo, start, end }, res) {
  let connection;

  try {
    connection = await getConnection();

    const { sql, binds } = buildCartolaQuery({ tipo, start, end });
    const result = await connection.execute(sql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    const rawRows = result.rows || [];
    let calculatedRows;

    if (tipo === 'LDN') {
      calculatedRows = rawRows;
    } else {
      calculatedRows = rawRows.map((r) => {
        // --- NUEVA LÓGICA BASADA EN LA CORRECCIÓN PREVIA ---
        const montoAbonadoHoy = r.MONTO; // Puede ser 0 si es abono futuro
        const montoCuotaFisica = r.MONTO_CUOTA_REAL || (montoAbonadoHoy > 0 ? montoAbonadoHoy : 0);
        const ventaTotalOriginal = r.VENTA_TOTAL_ORIGINAL;

        let cuota = r.CUOTA;
        let totalCuotas = r.TOTAL_CUOTAS;
        const isLcnFullPayment = (r.RETE || '').trim() === '0000';

        if (isLcnFullPayment || !totalCuotas || totalCuotas === 0) {
          cuota = 1;
          totalCuotas = 1;
        }

        // 1. MONTO_VENTA: Usamos el valor real de la transacción, no el abono del mes
        const montoVenta = ventaTotalOriginal || montoCuotaFisica * totalCuotas;

        // 2. DEUDA_PAGADA: Calculamos cuánto se ha pagado hasta esta cuota
        // Usamos montoCuotaFisica para que no se arruine si el abono actual es futuro (0)
        let deudaPagada = cuota * montoCuotaFisica;

        // 3. DEUDA_POR_PAGAR: Diferencia real
        let deudaPorPagar = Math.max(0, montoVenta - deudaPagada);

        // Limpieza de decimales por redondeo
        if (Math.abs(deudaPorPagar) <= 1) deudaPorPagar = 0;

        return {
          ID: r.ID,
          CUPON: r.CUPON,
          CODIGO_AUTORIZACION: r.CODIGO_AUTORIZACION,
          FECHA_VENTA: r.FECHA_VENTA,
          RUT: r.RUT || 'No se encuentra rut',
          MONTO: montoAbonadoHoy, // Mantenemos el abono real del periodo para el reporte
          CUOTA: cuota,
          TOTAL_CUOTAS: totalCuotas,
          CUOTAS_RESTANTES: Math.max(0, totalCuotas - cuota),
          MONTO_VENTA: montoVenta,
          DEUDA_PAGADA: deudaPagada,
          DEUDA_POR_PAGAR: deudaPorPagar,
          FECHA_ABONO: r.FECHA_ABONO,
          TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
        };
      });
    }

    const finalDataForExcel = calculatedRows.map((r) => {
      const record = {
        FECHA_VENTA: r.FECHA_VENTA,
        FECHA_ABONO: r.FECHA_ABONO,
        RUT: r.RUT,
        CUPON: r.CUPON,
        CODIGO_AUTORIZACION: r.CODIGO_AUTORIZACION,
        TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
      };

      if (tipo === 'LCN') {
        record.MONTO_VENTA = r.MONTO_VENTA;
        record.MONTO_ABONADO = r.MONTO; // Este será 0 en ventas de fin de mes (Correcto)
        record.CUOTA = r.CUOTA;
        record.DEUDA_PAGADA = r.DEUDA_PAGADA;
        record.DEUDA_POR_PAGAR = r.DEUDA_POR_PAGAR;
        record.TOTAL_CUOTAS = r.TOTAL_CUOTAS;
        record.CUOTAS_RESTANTES = r.CUOTAS_RESTANTES;
      } else {
        record.MONTO_ABONADO = r.MONTO;
      }
      return record;
    });

    await exportToExcel(finalDataForExcel, res, `Cartola_${tipo}.xlsx`);
  } catch (err) {
    console.error('Error exportando Excel:', err);
    if (!res.headersSent) res.status(500).json({ error: 'Error al exportar Excel' });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}
/**
 * Ayudante para formatear un objeto Date a 'DDMMYYYY'
 */
// Asumo que tienes esta función de ayuda en alguna parte
function formatFecha(date) {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${dd}${mm}${yyyy}`;
}

async function getDataHistorial({ rut, cupon, tipo }) {
  if (tipo !== 'LCN') {
    throw new Error('Tipo de registro no valido. Solo se soporta LCN.');
  }
  if (!cupon) {
    throw new Error('El parámetro "cupon" es requerido.');
  }

  const connection = await getConnection();
  const tabla_lcn = 'LCN_TBK_HISTORICO';
  let sql;

  // ... (tu SQL queda exactamente igual)
  switch (tipo) {
    case 'LCN':
      sql = `
          WITH
          LCN_Con_Cupon AS (
            SELECT
              a.*,
              (
                CASE
                  WHEN REGEXP_LIKE(TRIM(a.liq_orpedi), '^[0-9]*[1-9][0-9]*$') 
                  THEN LTRIM(TRIM(a.liq_orpedi), '0')
                  ELSE LTRIM(TRIM(a.liq_codaut), '0')
                END
              ) AS cupon_unificado
            FROM
              ${tabla_lcn}   a
          ),
          LCN_Con_ID_Venta AS (
            SELECT
              d.*,
              MAX(
                CASE
                  WHEN d.cupon_unificado = LTRIM(TRIM(:cupon), '0') 
                  THEN d.liq_orpedi
                  ELSE NULL
                END
              ) OVER () AS id_venta_objetivo
            FROM
              LCN_Con_Cupon d
          )
        SELECT
          d.cupon_unificado AS CUPON_CREDITO,
          :rut AS RUT,
          d.liq_fcom AS FECHA_VENTA,
          d.liq_fpago AS FECHA_ABONO,
          d.liq_cuotas AS CUOTA_PAGADA,
          d.liq_ntc AS TOTAL_CUOTAS,
          TRUNC(d.liq_monto / 100) AS MONTO,
          vec_cob01.pip_obtiene_nombre(:rut) AS NOMBRE
        FROM
          LCN_Con_ID_Venta d
        WHERE
          d.liq_orpedi = d.id_venta_objetivo
        ORDER BY
          d.liq_cuotas ASC
      `;
      break;
    default:
      throw new Error('Tipo de registro no valido');
  }

  let result;
  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { rut, cupon };

    result = await connection.execute(sql, binds, options);

    const cuotasPagadas = result.rows || [];

    if (cuotasPagadas.length === 0) {
      return [];
    }

    const datosBase = cuotasPagadas[0];
    const totalCuotas = datosBase.TOTAL_CUOTAS;
    const montoCuotaBase = datosBase.MONTO || 0;

    if (!totalCuotas || totalCuotas === 1) {
      const registroUnico = {
        ...datosBase,
        CUOTA_PAGADA: datosBase.CUOTA_PAGADA || 1,
        TOTAL_CUOTAS: totalCuotas || 1,
        MONTO: montoCuotaBase,
        CUOTAS_RESTANTES: 0,
        DEUDA_PAGADA: montoCuotaBase,
        DEUDA_POR_PAGAR: 0,
      };
      return [registroUnico];
    }

    const historialCompleto = [];
    let fechaObjBase;
    let diaBase;
    let cuotaBaseNum;

    for (let i = 1; i <= totalCuotas; i++) {
      const registroRealPagado = cuotasPagadas.find((c) => c.CUOTA_PAGADA === i);

      const cuotasRestantes = totalCuotas - i;
      const deudaPagadaAcumulada = i * montoCuotaBase;
      const deudaPorPagar = cuotasRestantes * montoCuotaBase;

      if (registroRealPagado) {
        historialCompleto.push({
          ...registroRealPagado,
          CUOTAS_RESTANTES: cuotasRestantes,
          DEUDA_PAGADA: deudaPagadaAcumulada,
          DEUDA_POR_PAGAR: deudaPorPagar,
        });
      } else {
        if (!fechaObjBase) {
          const fechaBaseStr = datosBase.FECHA_ABONO;
          cuotaBaseNum = datosBase.CUOTA_PAGADA;

          if (!fechaBaseStr) {
            console.error(
              `Error: No se puede proyectar la cuota ${i} porque datosBase.FECHA_ABONO es nula.`
            );
            continue;
          }

          diaBase = parseInt(fechaBaseStr.substring(0, 2), 10);
          const mesBase = parseInt(fechaBaseStr.substring(2, 4), 10) - 1;
          const anoBase = parseInt(fechaBaseStr.substring(4, 8), 10);
          fechaObjBase = new Date(anoBase, mesBase, diaBase);
        }

        const mesesDiferencia = i - cuotaBaseNum;
        const fechaProyectada = new Date(fechaObjBase.getTime());
        fechaProyectada.setDate(1);
        fechaProyectada.setMonth(fechaProyectada.getMonth() + mesesDiferencia);
        const maxDiaMesProyectado = new Date(
          fechaProyectada.getFullYear(),
          fechaProyectada.getMonth() + 1,
          0
        ).getDate();
        fechaProyectada.setDate(Math.min(diaBase, maxDiaMesProyectado));

        const fechaAbonoProyectada = formatFecha(fechaProyectada);

        historialCompleto.push({
          CUPON_CREDITO: datosBase.CUPON_CREDITO,
          RUT: datosBase.RUT,
          FECHA_VENTA: datosBase.FECHA_VENTA,
          FECHA_ABONO: fechaAbonoProyectada,
          CUOTA_PAGADA: i,
          TOTAL_CUOTAS: totalCuotas,
          MONTO: montoCuotaBase,
          NOMBRE: datosBase.NOMBRE,
          CUOTAS_RESTANTES: cuotasRestantes,
          DEUDA_PAGADA: deudaPagadaAcumulada,
          DEUDA_POR_PAGAR: deudaPorPagar,
        });
      }
    }

    return historialCompleto;
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
  getCartolaExcel,
  getTotalesWebpayPorDocumento,
  getCartolaDataAndTotals,
};
