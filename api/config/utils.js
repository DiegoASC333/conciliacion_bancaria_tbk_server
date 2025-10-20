const Client = require('ssh2-sftp-client');
const sftp = new Client();
require('dotenv').config();
const oracledb = require('oracledb');
const fs = require('fs');
const os = require('os');
const path = require('path');
const ExcelJS = require('exceljs');
// Inicializar modo Thick (esto habilita compatibilidad con versiones antiguas)
oracledb.initOracleClient({ libDir: '/opt/oracle' }); // Cambia según tu sistema

// Fuerza THICK explícitamente y apunta al Instant Client

//process.env.NODE_ORACLEDB_DRIVER_MODE = 'thick';
//oracledb.initOracleClient({ libDir: '/opt/oracle/instantclient' });

async function conectarSFTP({ host, port, username, password }) {
  await sftp.connect({
    host,
    port,
    username,
    password,
  });
  return { conn: sftp, sftp };
}
async function getConnection() {
  return await oracledb.getConnection({
    user: process.env.ORACLE_USER,
    password: process.env.ORACLE_PASSWORD,
    connectString: process.env.ORACLE_CONNECT_STRING,
  });
}

function obtenerFechaDesdeNombre(nombreArchivo) {
  const match = nombreArchivo.match(/_(\d{8})_/);
  if (!match) return null;

  const fechaStr = match[1]; // ejemplo: "28042025"
  const dia = fechaStr.slice(0, 2);
  const mes = fechaStr.slice(2, 4);
  const anio = fechaStr.slice(4, 8);

  const fecha = new Date(`${anio}-${mes}-${dia}`);
  return isNaN(fecha) ? null : fecha;
}

function stripGz(name) {
  return name.toLowerCase().endsWith('.gz') ? name.slice(0, -3) : name;
}

function mkTempFile(ext = '.dat') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'datgz-'));
  const file = path.join(dir, `tmp${ext}`);
  return { dir, file };
}

function cleanupTemp(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {}
}

function obtenerRangoDelDiaActual() {
  const hoy = new Date();

  // inicio = 00:00:00.000 local
  const start = new Date(hoy);
  start.setHours(0, 0, 0, 0);

  // fin = 00:00:00.000 del día siguiente (rango medio abierto [start, end))
  const end = new Date(start);
  end.setDate(end.getDate() + 1);

  return { start, end };
}

/**
 * Genera un Excel y lo envía directamente en la respuesta HTTP
 * @param {Array} rows - filas a exportar
 * @param {object} res - objeto response de Express
 * @param {string} filename - nombre del archivo
 */
async function exportToExcel(rows, res, filename = 'export.xlsx') {
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Datos');

  if (rows.length) {
    sheet.columns = Object.keys(rows[0]).map((k) => ({
      header: k,
      key: k,
      width: 20,
    }));
    rows.forEach((row) => sheet.addRow(row));
  }

  res.setHeader(
    'Content-Type',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
  );
  res.setHeader('Content-Disposition', `attachment; filename=${filename}`);

  await workbook.xlsx.write(res);
  res.end();
}

async function parseJSONLob(lob) {
  if (!lob) return null;

  return new Promise((resolve, reject) => {
    let json = '';
    lob.setEncoding('utf8'); // importante
    lob.on('data', (chunk) => (json += chunk));
    lob.on('end', () => resolve(json));
    lob.on('error', (err) => reject(err));
  });
}
// Función para obtener rangos de un día específico en formato que Oracle reconoce
function obtenerRangoDeFechaExacta(fechaStr) {
  const fecha = new Date(fechaStr);

  const pad = (n) => n.toString().padStart(2, '0');
  const startLCN = `${pad(fecha.getDate())}${pad(fecha.getMonth() + 1)}${fecha.getFullYear()}`;
  const endFecha = new Date(fecha);
  endFecha.setDate(endFecha.getDate() + 1);
  const endLCN = `${pad(endFecha.getDate())}${pad(endFecha.getMonth() + 1)}${endFecha.getFullYear()}`;

  const startLDN = `${pad(fecha.getDate())}/${pad(fecha.getMonth() + 1)}/${fecha.getFullYear().toString().slice(-2)}`;
  const endLDN = `${pad(endFecha.getDate())}/${pad(endFecha.getMonth() + 1)}/${endFecha.getFullYear().toString().slice(-2)}`;

  return { startLCN, endLCN, startLDN, endLDN };
}

function formatearFechaParaDB(fechaStr) {
  const fecha = new Date(fechaStr);

  const dd = String(fecha.getDate()).padStart(2, '0');
  const mm = String(fecha.getMonth() + 1).padStart(2, '0');
  const yyyy = fecha.getFullYear();
  const yy = String(yyyy).slice(-2);

  return {
    startLCN: `${dd}${mm}${yyyy}`, // Ej: 29052025
    startLDN: `${dd}/${mm}/${yy}`, // Ej: 29/05/25
  };
}

module.exports = {
  conectarSFTP,
  getConnection,
  obtenerFechaDesdeNombre,
  stripGz,
  mkTempFile,
  cleanupTemp,
  obtenerRangoDelDiaActual,
  exportToExcel,
  parseJSONLob,
  obtenerRangoDeFechaExacta,
  formatearFechaParaDB,
};
