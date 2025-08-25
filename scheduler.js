require('dotenv').config();
const cron = require('node-cron');
const axios = require('axios');

const API_URL = process.env.API_URL;
const TZ = process.env.TZ || 'America/Santiago';

let running = false;

async function runJob() {
  if (running) {
    console.log(new Date().toISOString(), 'Saltado: ya hay una ejecución en curso');
    return;
  }

  running = true;
  console.log(new Date().toISOString(), 'Iniciando job...');

  try {
    const res = await axios.post(
      API_URL,
      {
        fechaInicio: '2025-01-09', // <- valor fijo para pruebas
        fechaFin: '2025-01-10',
      },
      { timeout: 30 * 60 * 1000 }
    );
    console.log(new Date().toISOString(), 'OK', res.status);
  } catch (e) {
    console.error(new Date().toISOString(), 'ERROR', e?.response?.status || e.message);
  } finally {
    running = false;
  }
  // Programa a las 06:00 todos los días (hora de Chile)
  // cron.schedule('0 6 * * *', runJob, { timezone: TZ });

  console.log('Scheduler iniciado. Zona horaria:', TZ);
}

// (Opcional) Descomenta para probar de inmediato sin esperar a las 06:00
runJob();
