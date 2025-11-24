const {
  getDataCartola,
  getTotalesWebpay,
  getDataHistorial,
  getCartolaExcel,
  getTotalesWebpayPorDocumento,
} = require('../services/cartolaTesoreriaService');

const getCartolaTesoreriaController = async (req, res) => {
  try {
    const { tipo, start, end } = req.body;
    //const status = await getDataCartola({ tipo, start, end });

    const [detalle, totales] = await Promise.all([
      getDataCartola({ tipo, start, end }),
      getTotalesWebpay({ tipo, start, end }),
    ]);

    res.status(200).json({
      success: true,
      status: 200,
      data: {
        detalle_transacciones: detalle,
        totales: totales,
      },
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar por tip0', error: error.message });
  }
};

const formatFechaDDMMYYYY = (fecha) => {
  // Si la fecha es null o undefined (como en cuotas pendientes)
  if (!fecha) {
    return null;
  }

  // Convertir a string (por si viene como número) y rellenar con ceros
  const fechaStr = String(fecha).padStart(8, '0');

  const dia = fechaStr.substring(0, 2);
  const mes = fechaStr.substring(2, 4);
  const ano = fechaStr.substring(4, 8);

  // Evitar fechas inválidas como 00/00/0000
  if (dia === '00') {
    return null;
  }

  return `${dia}/${mes}/${ano}`;
};

const getDataHistorialRut = async (req, res) => {
  try {
    const { rut, cupon, tipo } = req.body;

    const historial = await getDataHistorial({ rut, cupon, tipo });

    const dataFormateada = historial.map((cuota) => ({
      ...cuota,
      FECHA_VENTA: formatFechaDDMMYYYY(cuota.FECHA_VENTA),
      FECHA_ABONO: formatFechaDDMMYYYY(cuota.FECHA_ABONO),
    }));

    res.status(200).json({
      success: true,
      status: 200,
      data: dataFormateada,
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al traer información', error: error.message });
  }
};

async function getCartolaxls(req, res) {
  const { tipo, start, end } = req.body;

  try {
    await getCartolaExcel({ tipo, start, end }, res);
  } catch (err) {
    console.error('Error en controller Excel:', err);
    res.status(500).json({ error: 'Error al generar Excel' });
  }
}

async function getTotalesPorDocumento(req, res) {
  try {
    const { tipo, start, end } = req.body;

    if (!tipo || !start || !end) {
      return res.status(400).json({
        message: 'Faltan parámetros obligatorios en el body: tipo, start y end.',
      });
    }
    const resultados = await getTotalesWebpayPorDocumento({ tipo, start, end });

    if (resultados.length === 0) {
      return res.status(404).json({
        message: 'No se encontraron datos para los parámetros proporcionados.',
      });
    }

    res.status(200).json({
      success: true,
      status: 200,
      data: resultados,
    });
  } catch (error) {
    console.error('Error en [ReportesController] getTotalesWebpayPorDocumento:', error);
    return res.status(500).json({
      message: 'Error interno del servidor al consultar los totales por documento.',
      error: error.message, // Opcional: enviar el mensaje de error
    });
  }
}

module.exports = {
  getCartolaTesoreriaController,
  getDataHistorialRut,
  getCartolaxls,
  getTotalesPorDocumento,
};
