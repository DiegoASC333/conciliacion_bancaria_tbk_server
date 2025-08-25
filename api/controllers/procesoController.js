/* eslint-disable no-undef */
const { moverRegistrosCCN } = require('../services/procesoService');

const guardarRegistrosCCN = async (req, res) => {
  try {
    const resultado = await moverRegistrosCCN();
    res.json(resultado);
  } catch (error) {
    res.status(500).json({ mensaje: 'Error al guardar registros CCN', error: error.message });
  }
};

module.exports = {
  guardarRegistrosCCN,
};
