// const { obtenerRangoDelDiaActual } = require('../config/utils');
const { getStatusDiarioCuadratura, listarPorTipo } = require('../services/statusCuadraturaService');

const getStatusCuadratura = async (req, res) => {
  try {
    //const { start, end } = obtenerRangoDelDiaActual();
    const { fecha, perfil } = req.params;
    const status = await getStatusDiarioCuadratura({ fecha, perfil });

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
    reprocesados: ['REPROCESO', 'RE-PROCESADO'],
    total: null,
  };

  try {
    const { fecha, tipo, tipoTransaccion, perfil } = req.params;
    const clave = (tipo || '').toLowerCase();

    if (!(clave in tipo_flag)) {
      return res.status(400).json({
        success: false,
        status: 400,
        message: `Flag desconocido: ${tipo}. Usa: ${Object.keys(tipo_flag).join(', ')}`,
      });
    }

    const estados = tipo_flag[clave];

    const params = {
      estados: estados,
      tipoTransaccion: tipoTransaccion ? tipoTransaccion.toUpperCase() : null,
      fecha: fecha,
      perfil: perfil,
    };

    const data = await listarPorTipo(params);
    return res.status(200).json({
      success: true,
      status: 200,
      data: data,
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

module.exports = {
  getStatusCuadratura,
  listarporTipo,
};
