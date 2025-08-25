const { leerYParsearArchivo } = require('../services/parserService');
const { insertarRegistros } = require('../services/insertService');

async function procesarArchivoIndividual({ nombreArchivo, tipoArchivo }) {
  // Leer el archivo desde el origen (SFTP o local)
  const registros = await leerYParsearArchivo(nombreArchivo, tipoArchivo);

  // Insertar registros con validación incluida
  const resultado = await insertarRegistros({
    tipo: tipoArchivo,
    registros,
    nombreArchivo,
  });

  return resultado;
}

module.exports = {
  procesarArchivoIndividual,
};
