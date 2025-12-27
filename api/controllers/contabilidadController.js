const {
  getDataDescargaExcel,
  getDataExcelPorDia,
  generarReporte,
} = require('../services/contabilidadService');
const getReporteTransacciones = async (req, res) => {
  try {
    const { fecha_inicio, fecha_fin } = req.body;

    if (!fecha_inicio || !fecha_fin) {
      return res.status(400).json({ message: 'Faltan los parámetros fecha_inicio o fecha_fin' });
    }

    const resultados = await getDataDescargaExcel({ fecha_inicio, fecha_fin });

    if (resultados.length === 0) {
      return res.status(200).json({
        success: true,
        status: 200,
        data: [],
        mensaje: 'No existen datos para el rango de fechas seleccionado.',
      });
    }

    return res.status(200).json({
      success: true,
      status: 200,
      data: resultados,
    });
  } catch (error) {
    console.error('Error en el controlador de reportes:', error);
    res.status(500).json({ message: 'Error al generar el reporte', error: error.message });
  }
};

async function getReportePorDia(req, res) {
  const { fecha } = req.body; // fecha es la cadena, ej: "01/10/2025"

  if (!fecha) {
    return res.status(400).json({ mensaje: 'Debe proporcionar una fecha para generar el Excel.' });
  }

  const parts = fecha.split('/');
  if (parts.length !== 3) {
    return res.status(400).json({ mensaje: 'Formato de fecha inválido. Se esperaba DD/MM/YYYY.' });
  }
  try {
    const datos = await getDataExcelPorDia({ fecha: fecha });

    if (datos.length === 0) {
      return res
        .status(404)
        .json({ mensaje: 'No se encontraron datos para la fecha seleccionada.' });
    }

    // Si necesitas el objeto Date para generar el reporte (generarReporte),
    // puedes crearlo aquí usando la cadena 'fecha'.
    const fechaConsultaParaReporte = new Date(parts[2], parts[1] - 1, parts[0]);

    const fileBuffer = await generarReporte(datos, fechaConsultaParaReporte);

    //res.setHeader('Content-Type', 'text/csv');
    //res.setHeader('Content-Disposition', 'attachment; filename=' + 'Reporte_SAP.csv');
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    res.setHeader('Content-Disposition', 'attachment; filename=' + 'Reporte_SAP.xlsx');
    res.send(fileBuffer);
  } catch (err) {
    res.status(500).json({ mensaje: 'Error al generar Excel', error: err.message });
  }
}

module.exports = {
  getReporteTransacciones,
  getReportePorDia,
};
