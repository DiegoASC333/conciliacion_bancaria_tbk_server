const { ejecutarReprocesoCupon } = require('../services/reprocesoCuponService');

async function reprocesoCuponController(req, res) {
  try {
    const cupon = req.body.cupon;
    const id = req.body.id;

    if (!cupon) {
      return res.status(400).json({ success: false, message: 'El campo cupón es obligatorio.' });
    }
    if (!id) {
      return res.status(400).json({ success: false, message: 'El campo id es obligatorio.' });
    }

    const r = await ejecutarReprocesoCupon(Number(cupon), Number(id));

    if (r.estado && r.estado.success === false) {
      return res.status(400).json({
        success: false,
        message: r.estado.mensaje,
        details: r.estado,
      });
    }

    res.json({
      success: true,
      message: 'Cupón reprocesado correctamente',
      estado: r.estado,
    });
  } catch (error) {
    console.error('Error', error);
    res.status(500).json({ success: false, message: 'Error en el servidor' });
  }
}

module.exports = { reprocesoCuponController };
