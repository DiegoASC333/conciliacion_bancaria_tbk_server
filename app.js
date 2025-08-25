// app.js
require('dotenv').config();
const express = require('express');
const app = express();
const cors = require('cors');

// Middlewares
app.use(cors());
app.use(express.json()); // Para aceptar JSON en body

// Rutas
app.use('/', require('./api/routes')); // Acceso directo (sin prefijo) a rutas

process.on('SIGINT', () => {
  console.log('\n[Proceso] SIGINT recibido. Cerrando aplicación...');
  process.exit(0); // Esto debe cerrar el proceso correctamente
});

module.exports = app;
