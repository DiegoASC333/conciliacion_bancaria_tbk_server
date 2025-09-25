const { getConnection, exportToExcel } = require('../config/utils');
const { buildLiquidacionQuery } = require('../config/queryBuilder');
const oracledb = require('oracledb');

async function getLiquidacion({ tipo, startLCN, startLDN }) {
  const connection = await getConnection();
  try {
    const { sql, binds } = buildLiquidacionQuery({ tipo, startLCN, startLDN });
    const res = await connection.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return res.rows || [];
  } finally {
    try {
      await connection.close();
    } catch {}
  }
}

async function getLiquidacionTotales({ tipo, startLCN, startLDN }) {
  const connection = await getConnection();

  const tipoUpper = (tipo || '').toUpperCase();

  let where = '';
  let binds = {};

  if (tipoUpper === 'LCN') {
    where = "TIPO_TRANSACCION = 'LCN' AND liq_fpago = :fecha";
    binds.fecha = startLCN;
  } else if (tipoUpper === 'LDN') {
    where = "TIPO_TRANSACCION = 'LDN' AND liq_fedi = :fecha";
    binds.fecha = startLDN;
  }

  let comercioExpr;
  if (tipoUpper === 'LCN') {
    comercioExpr = `CASE WHEN l.liq_cprin != 99999999 THEN TRIM(l.liq_cprin) ELSE TRIM(l.liq_numc) END`;
  } else {
    comercioExpr = `TRIM(l.liq_numc)`;
  }

  const sql = `
    SELECT
      ${comercioExpr}                           AS CODIGO_COMERCIO,
      c.NOMBRE_COMERCIO AS NOMBRE_COMERCIO,
      SUM(l.liq_monto / 100)                     AS TOTAL_MONTO
      FROM liquidacion_file_tbk l
      LEFT JOIN vec_cob04.codigo_comerico c
    ON c.codigo_comerico = ${comercioExpr}
    WHERE ${where}
    AND ${comercioExpr} NOT IN ('28208820', '48211418')
    GROUP BY ${comercioExpr}, c.NOMBRE_COMERCIO
    ORDER BY TOTAL_MONTO DESC
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    //const binds = { tipo: tipoUpper, startLCN, startLDN };
    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('Error al obtener totales de liquidación:', error);
    throw error;
  } finally {
    try {
      await connection.close();
    } catch (_) {}
  }
}

async function getLiquidacionExcel({ tipo, start, end }, res) {
  let connection;

  try {
    connection = await getConnection();

    // Construir query dinámica
    const { sql, binds } = buildLiquidacionQuery({ tipo, start, end });

    const result = await connection.execute(sql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    // Llamar al utils para exportar Excel
    await exportToExcel(result.rows, res, `liquidaciones_${tipo}.xlsx`);
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

module.exports = {
  getLiquidacion,
  getLiquidacionTotales,
  getLiquidacionExcel,
};
