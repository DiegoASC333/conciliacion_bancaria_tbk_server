const { obtenerFechaDesdeNombre } = require('../config/utils');
const { ejecutarValidaCupon } = require('../services/validaCuponService');
const { procesarCupones } = require('../services/procesarCuponesService');
const { procesarArchivosPorTipo } = require('../services/baseFileProcessor');
const { ejecutarScriptRemoto } = require('../services/sshService');

const procesarCuadraturas = async (req, res) => {
  const { fechaInicio, fechaFin } = req.body || {};
  const prefijosCuad = ['CCN', 'CDN'];

  try {
    await ejecutarScriptRemoto({
      host: process.env.SSH_HOST,
      port: process.env.SSH_PORT,
      username: process.env.SSH_USER,
      password: process.env.SSH_PASSWORD,
      rutaScript: process.env.REMOTE_SCRIPT,
    });

    const data = await procesarArchivosPorTipo({
      prefijos: prefijosCuad,

      filtroDinamico: (nombre) => {
        const fechaArchivo = obtenerFechaDesdeNombre(nombre);
        if (!fechaArchivo) return false;

        // Si hay rango de fechas en el request, lo respetamos
        if (fechaInicio && fechaFin) {
          const fi = new Date(fechaInicio);
          const ff = new Date(fechaFin);
          ff.setHours(23, 59, 59, 999);
          return fechaArchivo >= fi && fechaArchivo <= ff;
        }

        const hoy = new Date();
        hoy.setHours(0, 0, 0, 0);
        return fechaArchivo >= hoy;
      },

      // CALLBACK: Esto se ejecuta SOLO si hubo inserciones exitosas
      callbackPostProceso: async () => {
        const resultadosPost = { procesarCupones: null, validaCupon: null };

        try {
          await procesarCupones();
          resultadosPost.procesarCupones = { ok: true };
        } catch (e) {
          resultadosPost.procesarCupones = { ok: false, error: e.message };
        }

        try {
          const v = await ejecutarValidaCupon();
          resultadosPost.validaCupon = v.ok ? { ok: true } : { ok: false, error: v.error };
        } catch (e) {
          resultadosPost.validaCupon = { ok: false, error: e.message };
        }

        return resultadosPost;
      },
    });

    res.json({
      mensaje: 'Proceso de cuadraturas finalizado',
      ...data,
    });
  } catch (error) {
    console.error('Error en cuadraturasController:', error);
    res.status(500).json({ mensaje: 'Error general', detalle: error.message });
  }
};

module.exports = {
  procesarCuadraturas,
};
