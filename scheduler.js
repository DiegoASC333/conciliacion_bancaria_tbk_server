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

  try {
    const hoy = new Date();
    const manana = new Date(hoy);
    manana.setDate(hoy.getDate() + 1);

    const fechaInicio = hoy.toISOString().split('T')[0];
    const fechaFin = manana.toISOString().split('T')[0];

    const res = await axios.post(
      API_URL,
      {
        fechaInicio: fechaInicio,
        fechaFin: fechaFin,
      },
      { timeout: 60 * 60 * 1000 } // 1 hora
    );
    console.log(new Date().toISOString(), 'OK', res.status);
  } catch (e) {
    console.error(new Date().toISOString(), 'ERROR', e?.response?.status || e.message);
  } finally {
    running = false;
  }
}

cron.schedule('0 6 * * *', runJob, { timezone: TZ });

// (Opcional) Descomenta para probar de inmediato sin esperar a las 06:00
//runJob();
