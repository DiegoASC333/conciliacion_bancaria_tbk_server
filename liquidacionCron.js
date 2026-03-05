require('dotenv').config();
const cron = require('node-cron');
const axios = require('axios');

// Ruta manual al puerto 4000
const LIQUIDACIONES_ENDPOINT = `http://127.0.0.1:4000/procesar-liquidaciones`;

const TZ = process.env.TZ || 'America/Santiago';

let running = false;

async function runLiquidacionesJob() {
  if (running) {
    return;
  }

  running = true;
  console.log(`[${new Date().toLocaleString()}] Iniciando Job de Liquidaciones (Modo Autónomo)...`);

  try {
    // No enviamos fechas, el controlador buscará el "D+1" automáticamente
    const res = await axios.post(
      LIQUIDACIONES_ENDPOINT,
      {},
      { timeout: 3600000 } // 1 hora
    );
  } catch (e) {
    const status = e?.response?.status;
    const errorMsg = e?.response?.data?.mensaje || e.message;
    console.error(`[${new Date().toLocaleString()}] ERROR en Liquidaciones:`, status, errorMsg);
  } finally {
    running = false;
  }
}

cron.schedule('0 7 * * *', runLiquidacionesJob, { timezone: TZ });

// runLiquidacionesJob(); // Descomenta para probar ahora mismo
