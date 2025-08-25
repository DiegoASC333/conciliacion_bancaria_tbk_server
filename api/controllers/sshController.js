const { ejecutarScriptRemoto } = require('../services/sshService');

const ejecutarScript = async (req, res) => {
  try {
    const { code, output } = await ejecutarScriptRemoto();
    res.json({
      mensaje: `Script ejecutado con código ${code}`,
      salida: output,
    });
  } catch (error) {
    res.status(500).json({
      mensaje: 'Error en script remoto',
      detalle: error.message,
    });
  }
};

module.exports = { ejecutarScript };
