const { getConnection, parseJSONLob } = require('../config/utils');
const oracledb = require('oracledb');

async function getStatusDiarioCuadratura({ start, end }) {
  const connection = await getConnection();

  const sql = ` SELECT
    COUNT(*) AS TOTAL_DIARIO,
    SUM(TRUNC(DKTT_DT_AMT_1/100)) AS MONTO_TOTAL_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN 1 ELSE 0 END) AS APROBADOS_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_APROBADOS,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN 1 ELSE 0 END) AS RECHAZADOS_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_RECHAZADOS,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESO','RE-PROCESADO') THEN 1 ELSE 0 END) AS REPROCESADOS_DIARIO,
    SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESO','RE-PROCESADO') THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_REPROCESADOS
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
      monto_total_diario: Number(r.MONTO_TOTAL_DIARIO || 0),
      aprobados_diario: Number(r.APROBADOS_DIARIO || 0),
      monto_aprobados: Number(r.MONTO_APROBADOS || 0),
      rechazados_diario: Number(r.RECHAZADOS_DIARIO || 0),
      monto_rechazados: Number(r.MONTO_RECHAZADOS || 0),
      reprocesados_diario: Number(r.REPROCESADOS_DIARIO || 0),
      monto_reprocesados: Number(r.MONTO_REPROCESADOS || 0),
    };
  } catch (error) {
    console.error('error', error);
  } finally {
    try {
      if (connection) await connection.close();
    } catch (_) {}
  }
}

async function listarPorTipo({ estados, validarCupon = true, tipoTransaccion }) {
  const conn = await getConnection();
  try {
    const binds = {};
    const conditions = [];

    if (Array.isArray(estados) && estados.length > 0) {
      const bindNames = estados.map((_, i) => `:estado_${i}`).join(', ');
      estados.forEach((v, i) => (binds[`estado_${i}`] = String(v).toUpperCase()));
      conditions.push(`UPPER(NVL(c.STATUS_SAP_REGISTER, '')) IN (${bindNames})`);
    }

    // Filtro por tipo de transacción (NUEVO)
    if (tipoTransaccion) {
      conditions.push(`c.tipo_transaccion = :tipoTransaccion`);
      binds.tipoTransaccion = tipoTransaccion;
    }

    const isValid = (col) => `REGEXP_LIKE(TRIM(${col}), '^[0-9]*[1-9][0-9]*$')`;

    if (validarCupon) {
      conditions.push(
        `( ${isValid('c.DKTT_DT_NUMERO_UNICO')} OR ${isValid('c.DSK_ID_NRO_UNICO')} OR TRIM(c.DKTT_DT_APPRV_CDE) IS NOT NULL )`
      );
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const cuponExpr = `
      CASE
        WHEN ${isValid('c.DSK_ID_NRO_UNICO')}     THEN LTRIM(TRIM(c.DSK_ID_NRO_UNICO), '0')
        WHEN ${isValid('c.DKTT_DT_NUMERO_UNICO')} THEN LTRIM(TRIM(c.DKTT_DT_NUMERO_UNICO), '0')
        WHEN TRIM(c.DKTT_DT_APPRV_CDE) IS NOT NULL THEN TRIM(c.DKTT_DT_APPRV_CDE)
        ELSE NULL
      END
    `;

    const sql = `SELECT
        ${cuponExpr}                       AS CUPON,
        p.rut AS RUT,
        vec_cob01.pip_obtiene_nombre(p.rut) as NOMBRE, 
        TRUNC(c.DKTT_DT_AMT_1/100)           AS MONTO_TRANSACCION,
        CASE 
          WHEN c.DKTT_DT_CANTI_CUOTAS IS NULL THEN 0 
          ELSE c.DKTT_DT_CANTI_CUOTAS 
        END AS CUOTAS,
        CASE 
          WHEN REGEXP_LIKE(TRIM(c.DKTT_DT_FECHA_PAGO), '^[0-9]{6}$')
            THEN TO_CHAR(TO_DATE(TRIM(c.DKTT_DT_FECHA_PAGO), 'RRMMDD'), 'YYYYMMDD')
        END AS FECHA_ABONO,
        CASE 
          WHEN REGEXP_LIKE(TRIM(c.DKTT_DT_TRAN_DAT), '^[0-9]{6}$')
            THEN TO_CHAR(TO_DATE(TRIM(c.DKTT_DT_TRAN_DAT), 'RRMMDD'), 'YYYYMMDD')
        END AS FECHA_VENTA, 
        CASE
          WHEN c.tipo_transaccion = 'CCN' THEN 'Crédito'
          WHEN c.tipo_transaccion = 'CDN' THEN 'Débito'
          ELSE c.tipo_transaccion
        END AS TIPO_TRANSACCION
      FROM cuadratura_file_tbk c
      LEFT JOIN proceso_cupon p ON TO_CHAR(p.cupon) = ${cuponExpr}
      ${whereClause}
      ORDER BY 
      CASE 
        WHEN REGEXP_LIKE(${cuponExpr}, '^[0-9]+$') THEN 1 ELSE 2 
      END, 
      CASE
        WHEN REGEXP_LIKE(${cuponExpr}, '^[0-9]+$') THEN LPAD(${cuponExpr}, 20, '0')
        ELSE ${cuponExpr}
      END, 
      c.date_load_bbdd DESC NULLS LAST
    `;

    const res = await conn.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });

    const filas = await Promise.all(
      (res.rows || []).map(async (r) => {
        let fecha_vencimiento = 'No encontrado';
        let carrera = 'No encontrado';
        let nombre_carrera = 'No encontrado';
        let tipo_documento = 'No encontrado';

        if (r.JSON_DATA) {
          try {
            let jsonStr;

            // si es un LOB, primero leerlo
            if (r.JSON_DATA instanceof oracledb.Lob) {
              jsonStr = await parseJSONLob(r.JSON_DATA);
            } else {
              jsonStr = r.JSON_DATA; // si ya es string
            }

            const obj = JSON.parse(jsonStr);
            const data = obj.data && typeof obj.data === 'object' ? obj.data : {};
            fecha_vencimiento = data.FECHA_VENCIMIENTO ?? 'No encontrado';
            nombre_carrera = data.NOMBRE_CARRERA ?? 'No encontrado';
            carrera = data.CARRERA ?? 'No encontrado';
            tipo_documento = data.TIPO_DOCUEMNTO ?? 'No encontrado';
          } catch (err) {
            console.error('Error parseando JSON:', r.JSON_DATA, err);
          }
        }

        return {
          RUT: r.RUT,
          NOMBRE: r.NOMBRE,
          CUOTAS: r.CUOTAS,
          CUPON: r.CUPON,
          FECHA_ABONO: r.FECHA_ABONO,
          FECHA_VENTA: r.FECHA_VENTA,
          MONTO_TRANSACCION: r.MONTO_TRANSACCION,
          // Campos extraídos del JSON
          fecha_vencimiento,
          nombre_carrera,
          carrera,
          tipo_documento,
          TIPO_TRANSACCION: r.TIPO_TRANSACCION,
        };
      })
    );

    return filas;
    //return res.rows || [];
  } finally {
    try {
      await conn.close();
    } catch {}
  }
}

module.exports = {
  getStatusDiarioCuadratura,
  listarPorTipo,
};
