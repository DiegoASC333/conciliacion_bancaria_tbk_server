const { procesarArchivoIndividual } = require('../services/archivoManualService');

const procesarArchivoPorNombre = async (req, res) => {
  const { nombreArchivo, tipoArchivo } = req.body;

  try {
    const resultado = await procesarArchivoIndividual({ nombreArchivo, tipoArchivo });
    res.status(200).json(resultado);
  } catch (error) {
    res.status(500).json({
      mensaje: `Error al procesar archivo ${nombreArchivo}`,
      error: error.message,
    });
  }
};

module.exports = {
  procesarArchivoPorNombre,
};
