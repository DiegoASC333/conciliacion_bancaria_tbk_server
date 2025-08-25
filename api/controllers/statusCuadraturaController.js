const { obtenerRangoDelDiaActual } = require('../config/utils');
const { getStatusDiarioCuadratura, listarPorTipo } = require('../services/statusCuadraturaService');

const getStatusCuadratura = async (req, res) => {
  try {
    const { start, end } = obtenerRangoDelDiaActual();
    const status = await getStatusDiarioCuadratura({ start, end });

    return res.status(200).json({
      success: true,
      status: 200,
      data: status,
    });
  } catch (error) {
    console.error('Error al obtener el estado de la cuadratura:', error);
    return res.status(500).json({
      success: false,
      status: 500,
      mensaje: 'Error al obtener el estado de la cuadratura',
      msjError: error.message,
    });
  }
};

const listarporTipo = async (req, res) => {
  const tipo_flag = {
    aprobados: ['ENCONTRADO'],
    rechazados: ['NO EXISTE', 'PENDIENTE'],
    reprocesados: ['REPROCESADO', 'RE-PROCESADO'],
    total: ['total'],
  };

  try {
    const { tipo } = req.params;
    const estado = tipo_flag[tipo?.toLowerCase()];
    if (!estado) {
      return res
        .status(400)
        .json(fail(`Flag desconocido: ${flag}. Usa: ${Object.keys(tipo_flag).join(', ')}`));
    }
    const status = await listarPorTipo({ tipo: estado });
    return res.status(200).json({
      success: true,
      status: 200,
      data: status,
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

module.exports = {
  getStatusCuadratura,
  listarporTipo,
};
