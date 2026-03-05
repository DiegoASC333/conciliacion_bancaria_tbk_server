require('dotenv').config();
const cron = require('node-cron');
const axios = require('axios');

const CUADRATURAS_ENDPOINT = `http://127.0.0.1:4000/procesar-cuadraturas`;

const TZ = process.env.TZ || 'America/Santiago';

let running = false;

async function runCuadraturasJob() {
  if (running) {
    console.log(`[${new Date().toLocaleString()}] Saltado: Proceso de cuadraturas en curso`);
    return;
  }

  running = true;

  try {
    // Calculamos fechas por si el controlador las requiere como respaldo
    const hoy = new Date();
    const fechaInicio = hoy.toISOString().split('T')[0];

    // En las cuadraturas, a veces basta con enviar el día de hoy
    const res = await axios.post(
      CUADRATURAS_ENDPOINT,
      {
        fechaInicio: fechaInicio,
        fechaFin: fechaInicio, // Procesamos lo del día
      },
      { timeout: 3600000 } // 1 hora de margen para el proceso pesado de Oracle
    );
  } catch (e) {
    const status = e?.response?.status;
    const errorMsg = e?.response?.data?.mensaje || e.message;
    console.error(`[${new Date().toLocaleString()}] ERROR en Cuadraturas:`, status, errorMsg);
  } finally {
    running = false;
  }
}

cron.schedule('0 6 * * *', runCuadraturasJob, { timezone: TZ });

// Prueba inmediata (descomenta para testear)
// runCuadraturasJob();
