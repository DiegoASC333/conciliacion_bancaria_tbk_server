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

      let cuotasRestantes, deudaPagada, deudaPorPagar;

      if (!totalCuotas || totalCuotas === 0) {
        cuota = 1;
        totalCuotas = 1;
        cuotasRestantes = 0;
        deudaPagada = monto;
        deudaPorPagar = 0;
      } else {
        cuotasRestantes = totalCuotas - cuota;
        deudaPagada = (cuota * monto) / totalCuotas;
        deudaPorPagar = monto - deudaPagada;
      }

      return {
        CUPON: r.CUPON,
        FECHA_VENTA: r.FECHA_VENTA,
        RUT: r.RUT,
        MONTO: monto,
        CUOTA: cuota,
        TOTAL_CUOTAS: totalCuotas,
        CUOTAS_RESTANTES: cuotasRestantes,
        DEUDA_PAGADA: deudaPagada,
        DEUDA_POR_PAGAR: deudaPorPagar,
        FECHA_ABONO: r.FECHA_ABONO,
        NOMBRE_BANCO: r.NOMBRE_BANCO,
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
        SUM(liq.liq_monto / 100) AS saldo_estimado,
        SUM(
          (NVL(liq.liq_ntc,0) - NVL(liq.liq_cuotas,0)) * (NVL(liq.liq_monto,0) / 100)
        ) AS saldo_por_cobrar
      FROM LCN_TBK_HISTORICO liq -- <<< CAMBIO DE TABLA
      JOIN vec_cob02.webpay_trasaccion wt
        ON TO_NUMBER(TRIM(liq.liq_orpedi)) = TO_NUMBER(TRIM(wt.id_sesion))
      WHERE REGEXP_LIKE(TRIM(liq.liq_orpedi), '^\\d+$')
        AND REGEXP_LIKE(TRIM(liq.liq_fpago), '^\\d{8}$')
        AND TO_DATE(TRIM(liq.liq_fpago), 'DDMMYYYY')
            BETWEEN TO_DATE(:fecha_ini, 'DDMMYYYY')
                AND TO_DATE(:fecha_fin, 'DDMMYYYY')
    `;
  } else if (tipo === 'LDN') {
    sql = `
      SELECT
        SUM(liq.liq_amt_1 / 100) AS saldo_estimado, -- <<< NUEVA COLUMNA
        0 AS saldo_por_cobrar -- Para débito, el saldo por cobrar es siempre 0
      FROM LDN_TBK_HISTORICO liq -- <<< CAMBIO DE TABLA
      JOIN vec_cob02.webpay_trasaccion wt
        ON TO_NUMBER(TRIM(liq.liq_nro_unico)) = TO_NUMBER(TRIM(wt.id_sesion)) -- <<< Se asume liq_orpedi existe para el JOIN
      WHERE REGEXP_LIKE(TRIM(liq.liq_nro_unico), '^\\d+$')
        AND TO_DATE(TRIM(liq.liq_fedi), 'DD/MM/YY') -- <<< NUEVA COLUMNA Y FORMATO
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
    return res.rows.length > 0 ? res.rows : [{ saldo_estimado: 0, saldo_por_cobrar: 0 }];
  } catch (error) {
    console.error('error en getTotalesWebpay', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
};

async function getDataHistorial({ rut, tipo }) {
  const connection = await getConnection();
  const tabla_lcn = 'LCN_TBK_HISTORICO';
  const tabla_ldn = 'LDN_TBK_HISTORICO';
  let sql;

  switch (tipo) {
    case 'LCN':
      sql = `
        SELECT
            CASE
                WHEN a.liq_orpedi IS NOT NULL
                    AND a.liq_orpedi > 0
                    AND REGEXP_LIKE(a.liq_orpedi, '^[1-9][0-9]*$')
                THEN a.liq_orpedi
                ELSE a.liq_codaut
            END AS cupon_Credito,
            b.rut AS rut,
            a.liq_fcom AS fecha_venta,
            a.liq_fpago AS fecha_abono,
            a.liq_cuotas AS cuota_pagada,
            a.liq_ntc AS TOTAL_CUOTAS,
            TRUNC(a.liq_monto / 100) AS monto,
            vec_cob01.pip_obtiene_nombre(b.rut) AS nombre,
            NVL(a.liq_ntc, 0) - NVL(a.liq_cuotas, 0) AS cuotas_restantes,
            CASE WHEN NVL(a.liq_cuotas,0) > 0
                  THEN (a.liq_monto / 100) / a.liq_cuotas
                  ELSE a.liq_monto
              END AS deuda_pagada,
            (NVL(a.liq_ntc,0) - NVL(a.liq_cuotas,0)) * (NVL(a.liq_monto,0) / 100) AS deuda_por_pagar
        FROM
            ${tabla_lcn} a,
            proceso_cupon b
        WHERE
            (a.liq_orpedi = b.cupon OR a.liq_codaut = b.cupon)
            AND b.rut = :rut`;
      break;

    case 'LDN':
      sql = `
        SELECT
            CASE
                WHEN a.liq_nro_unico IS NOT NULL
                    AND a.liq_nro_unico > 0
                    AND REGEXP_LIKE(a.liq_nro_unico, '^[1-9][0-9]*$')
                THEN a.liq_nro_unico
                ELSE a.liq_appr
            END AS cupon_Credito, -- Alias estandarizado
            b.rut AS rut,
            vec_cob01.pip_obtiene_nombre(b.rut) AS nombre,
            a.liq_fcom AS fecha_venta,
            a.liq_fedi AS fecha_abono,
            TRUNC(a.liq_amt_1 / 100) AS monto,
            1 AS cuota_pagada,
            1 AS TOTAL_CUOTAS,
            0 AS cuotas_restantes,
            TRUNC(a.liq_amt_1 / 100) AS deuda_pagada,
            0 AS deuda_por_pagar
        FROM
            ${tabla_ldn} a,
            proceso_cupon b
        WHERE
            (a.liq_nro_unico = b.cupon OR a.liq_appr = b.cupon)
            AND b.rut = :rut`;
      break;

    default:
      throw new Error('Tipo de registro no valido');
  }

  let result;
  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { rut };
    result = await connection.execute(sql, binds, options);
    return result.rows || [];
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

async function getCartolaExcel({ tipo, start, end }, res) {
  let connection;

  try {
    connection = await getConnection();

    // Construir query dinámica
    const { sql, binds } = buildCartolaQuery({ tipo, start, end });

    const result = await connection.execute(sql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    // Llamar al utils para exportar Excel
    await exportToExcel(result.rows, res, `Cartola_${tipo}.xlsx`);
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

// servicio temporal para pruebas
async function getDataHistorialMock({ rut, tipo }) {
  // Datos de prueba, todos con el mismo rut
  const datosFicticios = [
    {
      cupon_Credito: 743309,
      rut: rut, // mismo rut que se envía
      fecha_venta: '14052025',
      fecha_abono: '16052025',
      cuota_pagada: 1,
      TOTAL_CUOTAS: 5,
      monto: 30205,
      nombre: 'Juan Pérez',
      cuotas_restantes: 4,
      deuda_pagada: 30205,
      deuda_por_pagar: 151025,
    },
    {
      cupon_Credito: 743310,
      rut: rut,
      fecha_venta: '15052025',
      fecha_abono: '17052025',
      cuota_pagada: 2,
      TOTAL_CUOTAS: 5,
      monto: 50000,
      nombre: 'Juan Pérez',
      cuotas_restantes: 3,
      deuda_pagada: 25000,
      deuda_por_pagar: 75000,
    },
    {
      cupon_Credito: 743311,
      rut: rut,
      fecha_venta: '16052025',
      fecha_abono: '18052025',
      cuota_pagada: 3,
      TOTAL_CUOTAS: 5,
      monto: 45000,
      nombre: 'Juan Pérez',
      cuotas_restantes: 2,
      deuda_pagada: 30000,
      deuda_por_pagar: 60000,
    },
  ];

  return datosFicticios;
}

module.exports = {
  getDataCartola,
  getTotalesWebpay,
  getDataHistorial,
  getCartolaExcel,
  getDataHistorialMock,
};
