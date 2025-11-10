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

      let cuotasRestantes, deudaPagada, deudaPorPagar, montoVenta;

      if (!totalCuotas || totalCuotas === 0) {
        cuota = 1;
        totalCuotas = 1;
        cuotasRestantes = 0;
        deudaPagada = monto;
        deudaPorPagar = 0;
      } else {
        cuotasRestantes = totalCuotas - cuota;
        deudaPagada = cuota * monto;
        montoVenta = monto * totalCuotas;
        deudaPorPagar = montoVenta - deudaPagada;
      }

      return {
        ID: r.ID,
        CUPON: r.CUPON,
        FECHA_VENTA: r.FECHA_VENTA,
        RUT: r.RUT,
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

  if (tipo === 'LCN') {
    sql = `
      SELECT
        /* Saldo estimado: Es la suma del monto de las cuotas liquidadas en el rango */
        SUM(liq.LIQ_MONTO / 100) AS saldo_estimado,
        
        /* Saldo por cobrar: (Total cuotas - cuota actual) * valor de la cuota */
        SUM(
          (NVL(liq.LIQ_NTC, 0) - NVL(liq.LIQ_CUOTAS, 0)) * (NVL(liq.LIQ_MONTO, 0) / 100)
        ) AS saldo_por_cobrar
        
      FROM LCN_TBK_HISTORICO liq -- <<< Tabla histórica LCN
      WHERE
        /* Filtro de fecha usando la columna de LCN */
        REGEXP_LIKE(TRIM(liq.LIQ_FPAGO), '^[0-9]{8}$')
        AND TO_DATE(TRIM(liq.LIQ_FPAGO), 'DDMMYYYY')
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
      /* NO HAY JOIN A WEBPAY_TRASACCION */
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
        /* Filtro de fecha usando la columna de LDN */
        REGEXP_LIKE(TRIM(liq.LIQ_FEDI), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
        AND TO_DATE(TRIM(liq.LIQ_FEDI), 'DD/MM/RR') -- <<< Usamos DD/MM/RR por seguridad
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
      /* NO HAY JOIN A WEBPAY_TRASACCION */
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
        let cuotasRestantes, deudaPagada, deudaPorPagar, montoVenta;

        if (!totalCuotas || totalCuotas === 0) {
          cuota = 1;
          totalCuotas = 1;
          cuotasRestantes = 0;
          deudaPagada = monto;
          deudaPorPagar = 0;
        } else {
          cuotasRestantes = totalCuotas - cuota;
          deudaPagada = cuota * monto;
          montoVenta = monto * totalCuotas;
          deudaPorPagar = montoVenta - deudaPagada;
        }

        return {
          ID: r.ID,
          CUPON: r.CUPON,
          FECHA_VENTA: r.FECHA_VENTA,
          RUT: r.RUT,
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
function formatFecha(date) {
  const dia = String(date.getDate()).padStart(2, '0');
  const mes = String(date.getMonth() + 1).padStart(2, '0'); // Meses son 0-11
  const ano = date.getFullYear();
  return `${dia}${mes}${ano}`;
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
    const montoCuotaBase = datosBase.MONTO;
    const historialCompleto = [];

    const fechaBaseStr = datosBase.FECHA_ABONO;
    const cuotaBaseNum = datosBase.CUOTA_PAGADA;

    const diaBase = parseInt(fechaBaseStr.substring(0, 2), 10);
    const mesBase = parseInt(fechaBaseStr.substring(2, 4), 10) - 1;
    const anoBase = parseInt(fechaBaseStr.substring(4, 8), 10);
    const fechaObjBase = new Date(anoBase, mesBase, diaBase);

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
};
