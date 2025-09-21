const {
  getDataCartola,
  getTotalesWebpay,
  getDataHistorial,
  getCartolaExcel,
  getDataHistorialMock,
} = require('../services/cartolaTesoreriaService');

const getCartolaTesoreriaController = async (req, res) => {
  try {
    const { tipo, start, end } = req.body;
    //const status = await getDataCartola({ tipo, start, end });

    const [detalle, totales] = await Promise.all([
      getDataCartola({ tipo, start, end }),
      getTotalesWebpay({ tipo, start, end }),
    ]);

    res.status(200).json({
      success: true,
      status: 200,
      data: {
        detalle_transacciones: detalle,
        totales: totales,
      },
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

const getDataHistorialRut = async (req, res) => {
  try {
    const { rut, tipo } = req.body;

    const res = await getDataHistorial({ rut, tipo });

    res.status(200).json({
      success: true,
      status: 200,
      data: res,
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al traer información', error: error.message });
  }
};

async function getCartolaxls(req, res) {
  const { tipo, start, end } = req.body;

  try {
    await getCartolaExcel({ tipo, start, end }, res);
  } catch (err) {
    console.error('Error en controller Excel:', err);
    res.status(500).json({ error: 'Error al generar Excel' });
  }
}

async function getDataMock(req, res) {
  const { rut } = req.body;

  try {
    await getDataHistorialMock({ rut });
    res.status(200).json({
      success: true,
      status: 200,
      data: res,
    });
  } catch (err) {
    res.status(500).json({ error: 'Error al generar Excel' });
  }
}

module.exports = {
  getCartolaTesoreriaController,
  getDataHistorialRut,
  getCartolaxls,
  getDataMock,
};
