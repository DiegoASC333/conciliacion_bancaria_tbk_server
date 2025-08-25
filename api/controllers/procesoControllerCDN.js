/* eslint-disable no-undef */
const { moverRegistrosCDN } = require('../services/procesoServiceCDN');

const guardarRegistrosCDN = async (req, res) => {
  try {
    const resultado = await moverRegistrosCDN();
    res.json(resultado);
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al guardar registros CDN', error: error.message });
  }
};

module.exports = {
  guardarRegistrosCDN,
};
