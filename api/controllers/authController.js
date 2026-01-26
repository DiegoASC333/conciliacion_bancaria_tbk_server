const jwt = require('jsonwebtoken');
const authService = require('../services/authService');

const login = async (req, res) => {
  const { rut, password } = req.body;

  if (!rut || !password) {
    return res.status(400).json({ message: 'Faltan el RUT o la contraseña' });
  }

  try {
    const user = await authService.encontrarUsuario({ rut });

    if (!user || user.CLAVE !== password) {
      return res.status(401).json({ message: 'RUT o contraseña incorrectos' });
    }

    if (user.ACTIVO !== 1) {
      return res.status(403).json({ message: 'Usuario se encuentra desactivado' });
    }

    const payload = {
      rut: user.rut,
      rol: user.rol,
      perfil: user.perfil,
    };

    const token = jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: '8h' });

    res.status(200).json({
      message: 'Login exitoso',
      token: token,
      usuario: {
        rut: user.rut,
        rol: user.rol,
        perfil: user.perfil,
        activo: user.activo,
        nombre: user.nombre,
      },
    });
  } catch (error) {
    console.error('Error en el controlador de login:', error);
    res.status(500).json({ message: 'Error interno del servidor' });
  }
};

const loginBack = async (req, res) => {
  const { rut } = req.body;

  if (!rut) {
    return res.status(400).json({ message: 'El RUT es requerido.' });
  }

  try {
    const user = await authService.encontrarUsuario({ rut });

    if (!user) {
      return res.status(404).json({ message: 'Usuario no encontrado en el sistema.' });
    }

    const payload = {
      rut: user.RUT,
      rol: user.ROL,
      perfil: user.PERFIL,
    };

    const token = jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: '8h' });

    res.status(200).json({
      message: 'Login exitoso',
      token: token,
      usuario: {
        rut: user.rut,
        rol: user.rol,
        perfil: user.perfil,
        activo: user.activo,
        nombre: user.nombre,
      },
    });
  } catch (error) {
    console.error('Error en el controlador de login:', error);
    res.status(500).json({ message: 'Error interno del servidor' });
  }
};

module.exports = {
  login,
  loginBack,
};
