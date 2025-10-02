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
      rut: user.RUT,
      rol: user.ROL,
      perfil: user.PERFIL,
    };

    const token = jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: '8h' });

    res.status(200).json({
      message: 'Login exitoso',
      token: token,
      usuario: {
        rut: user.RUT,
        rol: user.ROL,
        perfil: user.PERFIL,
        activo: user.ACTIVO,
      },
    });
  } catch (error) {
    console.error('Error en el controlador de login:', error);
    res.status(500).json({ message: 'Error interno del servidor' });
  }
};

// authController.js (Backend)

const ssoValidate = async (req, res) => {
  const { ssoToken, rut } = req.body;

  if (!ssoToken || !rut) {
    return res.status(400).json({ message: 'Faltan el token o el rut del SSO.' });
  }

  try {
    //const ssoProviderUrl = `https://huemul.utalca.cl/sso/api/validate?token=${ssoToken}`;
    //const ssoProviderUrl = `https://api.utalca.cl/academia/sso/valida/${ssoToken}`;
    const ssoProviderUrl = `https://api.utalca.cl/academia/sso/valida/${rut}/${ssoToken}`;
    console.log(`[BACKEND]: Validando con la URL: ${ssoProviderUrl}`);
    const ssoResponse = await axios.get(ssoProviderUrl);
    console.log('[BACKEND]: Respuesta de la API de validación:', ssoResponse.data);

    if (ssoResponse.data && ssoResponse.data.valid && ssoResponse.data.rut) {
      const rutDesdeSSO = ssoResponse.data.rut;

      const user = await authService.encontrarUsuario({ rut: rutDesdeSSO });

      if (!user) {
        return res.status(404).json({ message: 'Usuario del SSO no encontrado en el sistema.' });
      }

      if (user.ACTIVO !== 1) {
        return res.status(403).json({ message: 'Usuario se encuentra desactivado.' });
      }

      const payload = {
        rut: user.RUT,
        rol: user.ROL,
        perfil: user.PERFIL,
      };
      const token = jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: '8h' });

      res.status(200).json({
        message: 'Login con SSO exitoso',
        token: token,
        usuario: {
          rut: user.RUT,
          rol: user.ROL,
          perfil: user.PERFIL,
          activo: user.ACTIVO,
        },
      });
    } else {
      return res.status(401).json({ message: 'Token del SSO inválido o expirado.' });
    }
  } catch (error) {
    console.error('Error en la validación SSO:', error.message);
    return res
      .status(500)
      .json({ message: 'Error interno del servidor durante la validación SSO.' });
  }
};

module.exports = {
  login,
  ssoValidate,
};
