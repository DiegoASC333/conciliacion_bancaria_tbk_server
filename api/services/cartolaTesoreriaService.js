const { getConnection, exportToExcel } = require('../config/utils');
const { buildCartolaQuery } = require('../config/queryBuilder');
const oracledb = require('oracledb');

async function getDataCartola({ tipo, start, end }) {
  const connection = await getConnection();

  try {
    const { sql, binds } = buildCartolaQuery({ tipo, start, end });

    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sql, binds, options);

    if (tipo === 'LDN') {
      return res.rows || [];
    }

    const filas = (res.rows || []).map((r) => {
      const monto = r.MONTO;
      let cuota = r.CUOTA;
      let totalCuotas = r.TOTAL_CUOTAS;
      let monto_total_venta = r.MONTO_TOTAL_VENTA;

      const isLcnFullPayment = tipo === 'LCN' && (r.RETE || '').trim() === '0000';

      if (isLcnFullPayment || !totalCuotas || totalCuotas === 0) {
        cuota = 1;
        totalCuotas = 1;
      }

      let cuotasRestantes, deudaPagada, deudaPorPagar, montoVenta;

      if (!totalCuotas || totalCuotas === 0) {
        cuota = 1;
        totalCuotas = 1;
        cuotasRestantes = 0;
        deudaPagada = monto;
        montoVenta = monto_total_venta;
        deudaPorPagar = 0;
      } else {
        cuotasRestantes = totalCuotas - cuota;
        deudaPagada = cuota * monto;
        montoVenta = monto_total_venta || monto * totalCuotas;
        deudaPorPagar = montoVenta - deudaPagada;
        if (deudaPorPagar < 0 && Math.abs(deudaPorPagar) <= 1) {
          deudaPorPagar = 0;
        } else if (deudaPorPagar > 0 && Math.abs(deudaPorPagar) <= 1) {
          deudaPorPagar = 0;
        }
      }

      return {
        ID: r.ID,
        CUPON: r.CUPON,
        FECHA_VENTA: r.FECHA_VENTA,
        RUT: r.RUT,
        MONTO: monto, //monto abonado, valor cuota o total de la venta segun corresponda
        CUOTA: cuota,
        TOTAL_CUOTAS: totalCuotas,
        CUOTAS_RESTANTES: cuotasRestantes,
        MONTO_VENTA: montoVenta,
        DEUDA_PAGADA: deudaPagada,
        DEUDA_POR_PAGAR: deudaPorPagar,
        FECHA_ABONO: r.FECHA_ABONO,
        TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
      };
    });

    return filas;
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
  let sql;

  const tipoUpper = (tipo || '').toUpperCase();
  const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

  if (tipo === 'LCN') {
    const lcn_l_dateFormat = 'DDMMYYYY';
    const lcn_h_dateFormat = 'RRMMDD';
    const nombreColumnaFechaPC = 'FECHA';

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

    sql = `
      SELECT
        /* Saldo estimado: Es la suma del monto de las cuotas liquidadas en el rango */
        SUM(liq.LIQ_MONTO / 100) AS saldo_estimado,
        
        /* Saldo por cobrar: (Total cuotas - cuota actual) * valor de la cuota */
        SUM(
          (NVL(liq.LIQ_NTC, 0) - NVL(liq.LIQ_CUOTAS, 0)) * (NVL(liq.LIQ_MONTO, 0) / 100)
        ) AS saldo_por_cobrar,
        SUM(h.DKTT_DT_AMT_1 / 100) AS saldo_total_ventas
        
      FROM LCN_TBK_HISTORICO liq 
      ${joinDocumento}
      ${joinProcesoCupon}
      WHERE
        REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$') -- <<< AGREGAR ESTA LÍNEA
        AND REGEXP_LIKE(TO_CHAR(liq.LIQ_FCOM), '^[0-9]{7,8}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.LIQ_FCOM), 8, '0'), 'DDMMYYYY')
        BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
        AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;

    // --- LÓGICA LDN (DÉBITO) ---
  } else if (tipo === 'LDN') {
    sql = `
      SELECT
        /* Saldo estimado: Suma de los montos de débito liquidados */
        SUM(liq.LIQ_AMT_1 / 100) AS saldo_estimado,
        
        /* Saldo por cobrar: Siempre 0 para débito */
        0 AS saldo_por_cobrar
        
      FROM LDN_TBK_HISTORICO liq -- <<< Tabla histórica LDN
      WHERE
        REGEXP_LIKE(TO_CHAR(liq.LIQ_FCOM), '^[0-9]{5,6}$')
        AND TO_DATE(LPAD(TO_CHAR(liq.LIQ_FCOM), 6, '0'), 'DDMMRR')
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;
  } else {
    return [{ saldo_estimado: 0, saldo_por_cobrar: 0 }];
  }

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = {
      fecha_ini: start,
      fecha_fin: end,
    };
    const res = await connection.execute(sql, binds, options);

    // Asegurar que devuelva 0 si SUM da NULL (ninguna fila encontrada)
    if (res.rows.length > 0 && res.rows[0].saldo_estimado !== null) {
      return res.rows;
    }
    return [{ saldo_estimado: 0, saldo_por_cobrar: 0 }];
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
          REGEXP_LIKE(TO_CHAR(liq.LIQ_FCOM), '^[0-9]{7,8}$')
          AND TO_DATE(LPAD(TO_CHAR(liq.LIQ_FCOM), 8, '0'), 'DDMMYYYY')
              BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                  AND TO_DATE(:fecha_fin, 'DDMMYYYY')
      `;

      sql = `
        SELECT
          ${labelExpr}             AS TIPO_DOCUMENTO,
          SUM(liq.LIQ_MONTO / 100) AS saldo_estimado,
          SUM(
            (NVL(liq.LIQ_NTC, 0) - NVL(liq.LIQ_CUOTAS, 0)) * (NVL(liq.LIQ_MONTO, 0) / 100)
          )                        AS saldo_por_cobrar
        FROM LCN_TBK_HISTORICO liq
        ${joinClause}
        ${whereClause}
        GROUP BY ${baseTipoDoc} 
        ORDER BY saldo_estimado DESC
      `;
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
        const monto = r.MONTO;
        let cuota = r.CUOTA;
        let totalCuotas = r.TOTAL_CUOTAS;
        let monto_total_venta = r.MONTO_TOTAL_VENTA;

        const isLcnFullPayment = tipo === 'LCN' && (r.RETE || '').trim() === '0000'; //

        if (isLcnFullPayment || !totalCuotas || totalCuotas === 0) {
          cuota = 1;
          totalCuotas = 1;
        }

        let cuotasRestantes, deudaPagada, deudaPorPagar, montoVenta;

        if (!totalCuotas || totalCuotas === 0) {
          cuota = 1;
          totalCuotas = 1;
          cuotasRestantes = 0;
          deudaPagada = monto;
          montoVenta = monto_total_venta;
          deudaPorPagar = 0;
        } else {
          cuotasRestantes = totalCuotas - cuota;
          deudaPagada = cuota * monto;
          montoVenta = monto_total_venta || monto * totalCuotas;
          deudaPorPagar = montoVenta - deudaPagada;
          if (deudaPorPagar < 0 && Math.abs(deudaPorPagar) <= 1) {
            deudaPorPagar = 0;
          } else if (deudaPorPagar > 0 && Math.abs(deudaPorPagar) <= 1) {
            deudaPorPagar = 0;
          }
        }

        return {
          ID: r.ID,
          CUPON: r.CUPON,
          FECHA_VENTA: r.FECHA_VENTA,
          RUT: r.RUT || 'No se encuentra rut',
          MONTO: monto,
          CUOTA: cuota,
          TOTAL_CUOTAS: totalCuotas,
          CUOTAS_RESTANTES: cuotasRestantes,
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
        TIPO_DOCUMENTO: r.TIPO_DOCUMENTO,
      };

      if (tipo === 'LCN') {
        record.MONTO_VENTA = r.MONTO_VENTA;
        record.MONTO_ABONADO = r.MONTO;
        record.CUOTA = r.CUOTA ?? null;
        record.DEUDA_PAGADA = r.DEUDA_PAGADA;
        record.DEUDA_POR_PAGAR = r.DEUDA_POR_PAGAR;
        record.TOTAL_CUOTAS = r.TOTAL_CUOTAS ?? null;
        record.CUOTAS_RESTANTES = r.CUOTAS_RESTANTES ?? null;
      } else if (tipo === 'LDN') {
        record.MONTO_ABONADO = r.MONTO;
      }
      return record;
    });
    await exportToExcel(finalDataForExcel, res, `Cartola_${tipo}.xlsx`);
  } catch (err) {
    console.error('Error exportando Excel:', err);
    res.status(500).json({ error: 'Error al exportar Excel' });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error('Error cerrando conexión:', err);
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
};
