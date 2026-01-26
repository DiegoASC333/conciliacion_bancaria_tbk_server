const {
  getSaldoPendienteService,
  getSaldoPendienteServiceExcel,
} = require('../services/SaldoPendienteService');

const SaldoPendienteController = async (req, res) => {
  try {
    // La fecha debe venir en formato DDMMYYYY en el cuerpo de la solicitud
    const { fecha, tipo } = req.body;

    // Validación simple del formato de fecha
    if (!fecha || !/^\d{8}$/.test(fecha)) {
      return res.status(400).json({
        success: false,
        mensaje: 'Se requiere una fecha válida (DDMMYYYY) en el cuerpo de la solicitud.',
      });
    }

    // Llamada al servicio que contiene la lógica de query y agrupación
    const { detalle_transacciones, totales } = await getSaldoPendienteService({ fecha, tipo });

    res.status(200).json({
      success: true,
      status: 200,
      data: {
        detalle_transacciones: detalle_transacciones,
        totales: totales,
      },
    });
  } catch (error) {
    // Manejo de errores
    console.error('Error al obtener el saldo pendiente LCN:', error);
    res.status(500).json({
      success: false,
      mensaje: 'Error interno del servidor al listar el saldo pendiente LCN',
      error: error.message,
    });
  }
};

async function getSaldoPendienteXls(req, res) {
  try {
    const { fecha, tipo } = req.body;

    if (!fecha || !/^\d{8}$/.test(fecha)) {
      return res.status(400).json({
        success: false,
        mensaje: 'Se requiere una fecha válida (DDMMYYYY) en el cuerpo de la solicitud.',
      });
    }
    await getSaldoPendienteServiceExcel({ fecha, tipo }, res);
  } catch (error) {
    console.error('Error al exportar el saldo pendiente a Excel:', error);
    if (!res.headersSent) {
      res.status(500).json({
        success: false,
        mensaje: 'Error interno del servidor al exportar el saldo pendiente a Excel',
        error: error.message,
      });
    }
  }
}

module.exports = { SaldoPendienteController, getSaldoPendienteXls };
