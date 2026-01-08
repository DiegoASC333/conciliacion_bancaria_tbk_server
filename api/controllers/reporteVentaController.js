const { obtenerVentas, getVentasExcel } = require('../services/reporteVentaService');

const getVentasController = async (req, res) => {
  const { tipo, start, end } = req.body;
  try {
    const { listado, totalVentas, resumenDocumentos } = await obtenerVentas({ tipo, start, end });
    res.status(200).json({
      succes: true,
      status: 200,
      total: totalVentas,
      resumenDocumentos: resumenDocumentos,
      data: listado,
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

async function getReportexls(req, res) {
  const { tipo, start, end } = req.body;

  try {
    await getVentasExcel({ tipo, start, end }, res);
  } catch (err) {
    console.error('Error en controller Excel:', err);
    res.status(500).json({ error: 'Error al generar Excel' });
  }
}

module.exports = {
  getVentasController,
  getReportexls,
};
