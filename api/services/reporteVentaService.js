const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');
const { buildVentasQuery } = require('../config/queryBuilder');

async function obtenerVentas({ tipo, start, end }) {
  const connection = await getConnection();

  try {
    const { sql, binds } = buildVentasQuery({ tipo, start, end });
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sql, binds, options);

    const filas = res.rows;

    const totalVentas = filas.reduce((acc, curr) => {
      return acc + (Number(curr.MONTO_VENTA) || 0);
    }, 0);

    const totalesPorDocumento = filas.reduce((acc, curr) => {
      const tipoDoc = curr.TIPO_DOCUMENTO || 'SIN TIPO';
      const monto = Number(curr.MONTO_VENTA) || 0;

      if (!acc[tipoDoc]) {
        acc[tipoDoc] = 0;
      }
      acc[tipoDoc] += monto;

      return acc;
    }, {});

    return {
      listado: filas,
      totalVentas: totalVentas,
      resumenDocumentos: totalesPorDocumento,
    };
  } catch (error) {
    console.error('error', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

module.exports = {
  obtenerVentas,
};
