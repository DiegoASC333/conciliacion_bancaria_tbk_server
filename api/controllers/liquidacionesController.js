const { obtenerFechaDesdeNombre } = require('../config/utils');
const { procesarArchivosPorTipo } = require('../services/baseFileProcessor');
const { obtenerUltimosRegistrosDB } = require('../services/liquidacionSearchService');
const { ejecutarScriptRemoto } = require('../services/sshService');

const procesarLiquidaciones = async (req, res) => {
  const prefijos = ['LCN', 'LDN'];
  const resultadosGlobales = [];

  try {
    await ejecutarScriptRemoto({
      host: process.env.SSH_HOST,
      port: process.env.SSH_PORT,
      username: process.env.SSH_USER,
      password: process.env.SSH_PASSWORD,
      rutaScript: process.env.REMOTE_SCRIPT,
    });

    const ultimos = await obtenerUltimosRegistrosDB(prefijos);

    for (const prefijo of prefijos) {
      const registro = ultimos.find((r) => r.TIPO_ARCHIVO === prefijo);

      let fechaUltima;
      if (registro) {
        fechaUltima = obtenerFechaDesdeNombre(registro.NOMBRE_ARCHIVO);
      } else {
        // Nativo: 7 días atrás (7 * 24 * 60 * 60 * 1000 ms)
        fechaUltima = new Date(Date.now() - 604800000);
      }

      // Nativo: Sumar 1 día y resetear a las 00:00:00
      const fechaObjetivo = new Date(fechaUltima);
      fechaObjetivo.setDate(fechaObjetivo.getDate() + 1);
      fechaObjetivo.setHours(0, 0, 0, 0);

      // Llamada al motor con filtro dinámico por fecha
      const data = await procesarArchivosPorTipo({
        prefijos: [prefijo],
        filtroDinamico: (nombre) => {
          const f = obtenerFechaDesdeNombre(nombre);
          return nombre.startsWith(prefijo) && f && f >= fechaObjetivo;
        },
      });

      resultadosGlobales.push({ prefijo, ...data });
    }

    res.json({ mensaje: 'Liquidaciones al día', detalles: resultadosGlobales });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

module.exports = {
  procesarLiquidaciones,
};
