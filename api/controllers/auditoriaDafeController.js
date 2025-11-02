const {
  enviarATesoreriaSoloSiSinPendientes,
  existenPendientesAnterioresA,
} = require('../services/auditoriaDafeService');

const postEnviarTesoreria = async (req, res) => {
  try {
    const { usuarioId, observacion, fecha, totalDiario, perfil } = req.body || {};

    if (!usuarioId)
      return res.status(400).json({ success: false, message: 'usuarioId es requerido' });

    if (!fecha) return res.status(400).json({ success: false, message: 'fecha es requerida' });

    const validacion = await existenPendientesAnterioresA({ fecha });

    if (validacion && validacion.existen) {
      return res.status(409).json({
        success: false,
        status: 409,
        message: `Existen registros pendientes (fecha más reciente: ${validacion.fechaMasReciente}). No se puede enviar a Tesorería.`,
      });
    }

    const { ok, cant, message } = await enviarATesoreriaSoloSiSinPendientes({
      usuarioId,
      observacion,
      fecha,
      totalDiario,
      perfil,
    });

    if (ok === false) {
      return res.status(400).json({
        success: false,
        status: 400,
        message: message || 'La operación de envío falló.',
      });
    }

    return res.status(200).json({
      success: true,
      status: 200,
      message: `Envío registrado. ${cant} registros enviados a Tesorería.`,
    });
  } catch (err) {
    res.status(err.status || 500).json({ success: false, message: err.message, code: err.code });
  }
};

const validarFechasAnteriores = async (req, res) => {
  try {
    const { fecha } = req.params;
    if (!fecha || fecha.length !== 6) {
      return res
        .status(400)
        .json({ success: false, message: 'El formato de fecha debe ser aammdd' });
    }

    const resultado = await existenPendientesAnterioresA({ fecha });

    return res.status(200).json({
      success: true,
      data: {
        existenPendientes: resultado.existen,
        // (Opcional) Podemos devolver la fecha más reciente con pendientes
        fechaMasReciente: resultado.fechaMasReciente,
      },
    });
  } catch (error) {
    console.error('Error al validar fechas anteriores:', error);
    return res.status(500).json({ success: false, message: 'Error interno al validar fechas' });
  }
};

module.exports = { postEnviarTesoreria, validarFechasAnteriores };
