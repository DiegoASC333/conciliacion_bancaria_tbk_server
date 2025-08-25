const { listarArchivosDatNuevos } = require('../services/sftpService');

const listarArchivosNuevos = async (req, res) => {
  try {
    const archivos = await listarArchivosDatNuevos();
    res.json({
      mensaje: `Se encontraron ${archivos.length} archivo(s) .dat nuevos`,
      archivos,
    });
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al listar archivos nuevos', error: error.message });
  }
};

module.exports = {
  listarArchivosNuevos,
};
