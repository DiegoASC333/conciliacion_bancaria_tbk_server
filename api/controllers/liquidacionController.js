const { getLiquidacion, getLiquidacionTotales } = require('../services/liquidaciónService');
const { obtenerRangoDelDiaActual } = require('../config/utils');

const getLiquidacionController = async (req, res) => {
  try {
    const { tipo } = req.body;
    const { start, end } = obtenerRangoDelDiaActual();
    // const status = await getLiquidacion({ tipo, start, end });

    // Llama a ambas funciones de forma paralela para mayor eficiencia
    const [detalles, totales] = await Promise.all([
      getLiquidacion({ tipo, start, end }),
      getLiquidacionTotales({ tipo, start, end }),
    ]);

    return res.status(200).json({
      success: true,
      status: 200,
      data: {
        detalles_transacciones: detalles,
        totales_por_comercio: totales,
      },
    });

    // return res.status(200).json({
    //   success: true,
    //   status: 200,
    //   data: status,
    // });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

module.exports = {
  getLiquidacionController,
};
