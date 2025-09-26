const {
  getLiquidacion,
  getLiquidacionTotales,
  getLiquidacionExcel,
  guardarLiquidacionesHistoricas,
} = require('../services/liquidaciónService');
const { obtenerRangoDelDiaActual, formatearFechaParaDB } = require('../config/utils');

const getLiquidacionController = async (req, res) => {
  try {
    const { tipo, fecha } = req.body;

    if (!fecha) {
      return res.status(400).json({ mensaje: 'Fecha no proporcionada.' });
    }

    let startLCN = null;
    let startLDN = null;

    if (tipo.toUpperCase() === 'LCN') {
      // De 2025-05-28 -> 28052025
      const [yyyy, mm, dd] = fecha.split('-');
      startLCN = `${dd}${mm}${yyyy}`;
      console.log(`Consulta LCN - fecha enviada: ${startLCN}`);
    } else if (tipo.toUpperCase() === 'LDN') {
      // De 2025-05-13 -> 13/05/25
      const [yyyy, mm, dd] = fecha.split('-');
      startLDN = `${dd}/${mm}/${yyyy.slice(2)}`;
      console.log(`Consulta LDN - fecha enviada: ${startLDN}`);
    }

    const [detalles, totales] = await Promise.all([
      getLiquidacion({ tipo, startLCN, startLDN }),
      getLiquidacionTotales({ tipo, startLCN, startLDN }),
    ]);

    if (!detalles || detalles.length === 0) {
      return res.status(200).json({
        success: true,
        status: 200,
        data: {
          detalles_transacciones: [],
          totales_por_comercio: [],
        },
        mensaje: 'No existen datos para la fecha seleccionada.',
      });
    }

    return res.status(200).json({
      success: true,
      status: 200,
      data: {
        detalles_transacciones: detalles,
        totales_por_comercio: totales,
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      mensaje: 'Error al listar por tipo',
      error: error.message,
    });
  }
};

async function getLiquidacionxls(req, res) {
  const { tipo } = req.body;
  const { start, end } = obtenerRangoDelDiaActual();

  try {
    // Llamamos al servicio, pasándole tipo, rango y la response para que exporte directamente
    await getLiquidacionExcel({ tipo, start, end }, res);
  } catch (err) {
    console.error('Error en controller Excel:', err);
    res.status(500).json({ error: 'Error al generar Excel' });
  }
}

const validarLiquidacionController = async (req, res) => {
  try {
    // 1. Extraemos los parámetros que envía el frontend desde el cuerpo (body) de la petición
    const { tipo, fecha, usuarioId } = req.body;

    // 2. Hacemos una validación básica de entrada
    if (!tipo || !fecha || !usuarioId) {
      return res.status(400).json({
        mensaje: 'Petición inválida. Se requiere "tipo", "fecha" y "usuarioId".',
      });
    }

    // 3. Llamamos a nuestra función de servicio, que hará todo el trabajo pesado
    const resultado = await guardarLiquidacionesHistoricas({ tipo, fecha, usuarioId });

    // 4. Si todo sale bien, enviamos una respuesta exitosa al frontend
    res.status(200).json({
      mensaje: 'Proceso de validación completado con éxito.',
      registrosProcesados: resultado.registrosProcesados,
    });
  } catch (error) {
    // 5. Si algo falla en el servicio, capturamos el error aquí
    console.error('Error en validarLiquidacionController:', error);

    // Enviamos un código de error específico si lo definimos (ej: 404 si no hay datos)
    if (error.status) {
      return res.status(error.status).json({ mensaje: error.message });
    }

    // Para cualquier otro error inesperado, enviamos un 500
    res.status(500).json({
      mensaje: 'Error interno del servidor al procesar la validación.',
      error: error.message,
    });
  }
};

module.exports = {
  getLiquidacionController,
  getLiquidacionxls,
  validarLiquidacionController,
};
