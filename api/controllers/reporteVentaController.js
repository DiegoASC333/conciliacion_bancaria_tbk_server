const { obtenerVentas } = require('../services/reporteVentaService');

const getVentasController = async (req, res) => {
  const { tipo, start, end } = req.body;
  try {
    const { listado, totalVentas, resumenDocumentos } = await obtenerVentas({ tipo, start, end });
    res.status(200).json({
      succes: true,
      status: 200,
      total: totalVentas, // <--- Nuevo campo con la sumatoria
      resumenDocumentos: resumenDocumentos,
      data: listado, // <--- El array de registros
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

module.exports = {
  getVentasController,
};
