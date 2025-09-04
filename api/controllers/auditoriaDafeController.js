const { enviarATesoreriaSoloSiSinPendientes } = require('../services/auditoriaDafeService');

const postEnviarTesoreria = async (req, res) => {
  try {
    const { usuarioId, observacion } = req.body || {};
    if (!usuarioId)
      return res.status(400).json({ success: false, message: 'usuarioId es requerido' });

    const { ok, cant } = await enviarATesoreriaSoloSiSinPendientes({ usuarioId, observacion });
    res.json({ success: ok, message: `Envío registrado. ${cant} registros enviados a Tesorería.` });
  } catch (err) {
    res.status(err.status || 500).json({ success: false, message: err.message, code: err.code });
  }
};
module.exports = { postEnviarTesoreria };
