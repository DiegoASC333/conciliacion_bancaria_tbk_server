const {
  getDataCartola,
  getTotalesWebpay,
  getDataHistorial,
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

module.exports = {
  getCartolaTesoreriaController,
  getDataHistorialRut,
};
