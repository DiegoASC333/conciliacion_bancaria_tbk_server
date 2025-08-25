const { leerYParsearArchivo } = require('../services/parserService');
const { insertarRegistros } = require('../services/insertService');

const procesarArchivo = async (req, res) => {
  const { nombreArchivo } = req.body;

  if (!nombreArchivo) {
    return res.status(400).json({ error: 'Debe especificar el nombre del archivo' });
  }

  try {
    const { tipo, registros } = await leerYParsearArchivo(nombreArchivo);
    const resultado = await insertarRegistros({ tipo, registros, nombreArchivo });

    res.json({
      mensaje: `Archivo procesado e insertado exitosamente.`,
      tipoArchivo: tipo,
      cantidad: resultado.cantidad,
    });
  } catch (error) {
    res
      .status(500)
      .json({ mensaje: 'Error al procesar o insertar el archivo', error: error.message });
  }
};

module.exports = {
  procesarArchivo,
};
