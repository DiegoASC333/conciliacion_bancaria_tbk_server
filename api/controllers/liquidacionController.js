const {
  getLiquidacion,
  getLiquidacionTotales,
  getLiquidacionExcel,
  guardarLiquidacionesHistoricas,
  findLatestPendingDate,
} = require('../services/liquidaciónService');

const getLiquidacionController = async (req, res) => {
  try {
    const { tipo, fecha } = req.body;

    if (!fecha) {
      return res.status(400).json({ mensaje: 'Fecha no proporcionada.' });
    }

    let advertencia = null;
    const pendingDate = await findLatestPendingDate({ tipo, fecha });

    if (pendingDate) {
      const formattedPendingDate = pendingDate.toISOString().split('T')[0];
      const mensaje = `Existen liquidaciones pendientes en la fecha ${formattedPendingDate}. Debe procesar esa fecha antes de continuar con ${fecha}.`;

      return res.status(409).json({
        success: false,
        status: 409,
        mensaje,
        data: {
          fecha_pendiente: formattedPendingDate,
        },
      });
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
  const { tipo, fecha } = req.body;

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
  //const { start, end } = obtenerRangoDelDiaActual();

  try {
    await getLiquidacionExcel({ tipo, startLCN, startLDN }, res);
  } catch (err) {
    console.error('Error en controller Excel:', err);
    res.status(500).json({ error: 'Error al generar Excel' });
  }
}

const validarLiquidacionController = async (req, res) => {
  try {
    const { tipo, fecha, usuarioId } = req.body;

    if (!tipo || !fecha || !usuarioId) {
      return res.status(400).json({
        mensaje: 'Petición inválida. Se requiere "tipo", "fecha" y "usuarioId".',
      });
    }

    const resultado = await guardarLiquidacionesHistoricas({ tipo, fecha, usuarioId });

    res.status(200).json({
      mensaje: 'Proceso de validación completado con éxito.',
      registrosProcesados: resultado.registrosProcesados,
    });
  } catch (error) {
    console.error('Error en validarLiquidacionController:', error);

    if (error.status) {
      return res.status(error.status).json({ mensaje: error.message });
    }

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
