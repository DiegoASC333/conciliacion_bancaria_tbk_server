const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function getStatusDiarioCuadratura({ start, end }) {
  const connection = await getConnection();

  const sql = ` SELECT
      COUNT(*) AS TOTAL_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN 1 ELSE 0 END) AS APROBADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN 1 ELSE 0 END) AS RECHAZADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESADO','RE-PROCESADO') THEN 1 ELSE 0 END) AS REPROCESADOS_DIARIO
    FROM CUADRATURA_FILE_TBK
     WHERE DATE_LOAD_BBDD >= :startDate
      AND DATE_LOAD_BBDD <  :endDate`;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { startDate: start, endDate: end }; // JS Date -> bind DATE/TIMESTAMP

    const res = await connection.execute(sql, binds, options);
    const r = res.rows?.[0] || {};

    return {
      total_diario: Number(r.TOTAL_DIARIO || 0),
      aprobados_diario: Number(r.APROBADOS_DIARIO || 0),
      rechazados_diario: Number(r.RECHAZADOS_DIARIO || 0),
      reprocesados_diario: Number(r.REPROCESADOS_DIARIO || 0),
    };
  } catch (error) {
    console.error('error', error);
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

// En tu servicio
async function listarPorTipo({ tipo }) {
  const connection = await getConnection();

  // 1. Validar que tipo sea un arreglo
  if (!Array.isArray(tipo) || tipo.length === 0) {
    throw new Error('El tipo debe ser un arreglo no vacío');
  }

  // 2. Crear una lista de placeholders para cada valor en el arreglo
  // Por ejemplo, para un array de 2 elementos, generaría ':tipo_0, :tipo_1'
  const bindNames = tipo.map((_, index) => `:tipo_${index}`).join(', ');

  // 3. Construir el objeto de binds con un nombre único para cada valor
  const binds = {};
  tipo.forEach((value, index) => {
    binds[`tipo_${index}`] = value;
  });

  // 4. Modificar la consulta para usar la cláusula IN
  const sql = `
    SELECT DKTT_DT_NUMERO_UNICO CUPON,
           DKTT_DT_AMT_1 MONTO_TRANSACCION,
           DKTT_DT_CARD NUMERO_TARJETA,
           DKTT_TIPO_CUOTA TIPO_CUOTA,
           DKTT_DT_CANTI_CUOTAS CUOTAS,
           DKTT_DT_FECHA_PAGO FECHA_ABONO,
           DKTT_DT_TRAN_DAT FECHA_TRANSACCION,
           date_load_bbdd FECHA_CARGA,
           tipo_transaccion
    FROM cuadratura_file_tbk
    WHERE STATUS_SAP_REGISTER IN (${bindNames})
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('Error al ejecutar la consulta:', error);
    throw error; // Propagar el error para que sea manejado en el controlador
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

module.exports = {
  getStatusDiarioCuadratura,
  listarPorTipo,
};
