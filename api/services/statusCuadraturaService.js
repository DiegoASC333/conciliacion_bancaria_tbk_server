const { getConnection, parseJSONLob } = require('../config/utils');
const oracledb = require('oracledb');
require('dotenv').config(); // Para cargar las variables del .env

async function getStatusDiarioCuadratura({ fecha, perfil }) {
  const connection = await getConnection();

  let perfilCondition = '';
  if (perfil) {
    if (perfil.toUpperCase() === 'FICA') {
      perfilCondition = ` AND centro_costo <> 'SD'`;
    } else if (perfil.toUpperCase() === 'SD') {
      perfilCondition = ` AND centro_costo = 'SD'`;
    }
  }

  const sqlAprobados = `
    SELECT
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN 1 ELSE 0 END) AS APROBADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_APROBADOS
    FROM CUADRATURA_FILE_TBK
    WHERE DKTT_DT_FECHA_VENTA = :fecha
    ${perfilCondition}  -- La condición del perfil se aplica AQUÍ
  `;

  const sqlOtros = `
    SELECT
      COUNT(*) AS TOTAL_DIARIO,
      SUM(TRUNC(DKTT_DT_AMT_1/100)) AS MONTO_TOTAL_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN 1 ELSE 0 END) AS RECHAZADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE') THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_RECHAZADOS,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESO','RE-PROCESADO') THEN 1 ELSE 0 END) AS REPROCESADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('REPROCESO','RE-PROCESADO') THEN TRUNC(DKTT_DT_AMT_1/100) ELSE 0 END) AS MONTO_REPROCESADOS
    FROM CUADRATURA_FILE_TBK
    WHERE DKTT_DT_FECHA_VENTA = :fecha
    -- Nótese la ausencia de la condición de perfil aquí
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const binds = { fecha: fecha };

    const [resAprobados, resOtros] = await Promise.all([
      connection.execute(sqlAprobados, binds, options),
      connection.execute(sqlOtros, binds, options),
    ]);

    const rAprobados = resAprobados.rows?.[0] || {};
    const rOtros = resOtros.rows?.[0] || {};

    return {
      total_diario: Number(rOtros.TOTAL_DIARIO || 0),
      monto_total_diario: Number(rOtros.MONTO_TOTAL_DIARIO || 0),

      aprobados_diario: Number(rAprobados.APROBADOS_DIARIO || 0),
      monto_aprobados: Number(rAprobados.MONTO_APROBADOS || 0),

      rechazados_diario: Number(rOtros.RECHAZADOS_DIARIO || 0),
      monto_rechazados: Number(rOtros.MONTO_RECHAZADOS || 0),
      reprocesados_diario: Number(rOtros.REPROCESADOS_DIARIO || 0),
      monto_reprocesados: Number(rOtros.MONTO_REPROCESADOS || 0),
    };
  } catch (error) {
    console.error('error', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (err) {
      console.error('Error al cerrar la conexión:', err);
    }
  }
}

async function listarPorTipo({ fecha, estados, validarCupon = true, tipoTransaccion, perfil }) {
  const conn = await getConnection();
  try {
    const binds = {};
    const conditions = [];

    if (Array.isArray(estados) && estados.length > 0) {
      const bindNames = estados.map((_, i) => `:estado_${i}`).join(', ');
      estados.forEach((v, i) => (binds[`estado_${i}`] = String(v).toUpperCase()));
      conditions.push(`UPPER(NVL(c.STATUS_SAP_REGISTER, '')) IN (${bindNames})`);
    }

    if (tipoTransaccion) {
      conditions.push(`c.tipo_transaccion = :tipoTransaccion`);
      binds.tipoTransaccion = tipoTransaccion;
    }

    if (fecha) {
      conditions.push(`c.DKTT_DT_FECHA_VENTA = :fecha`);
      binds.fecha = fecha;
    }

    const estadosQueAplicanFiltroPerfil = ['ENCONTRADO'];
    if (estadosQueAplicanFiltroPerfil && perfil) {
      const columnaTipoDocSap = 'sap.pade_tipo_documento';

      if (perfil.toUpperCase() === 'FICA') {
        conditions.push(`(${columnaTipoDocSap} NOT IN ('FA') OR ${columnaTipoDocSap} IS NULL)`);
      } else if (perfil.toUpperCase() === 'SD') {
        conditions.push(`${columnaTipoDocSap} IN ('FA')`);
      }
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

    const sql = `
    SELECT
        c.id as ID, 
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
        END AS TIPO_TRANSACCION,
        CASE
        WHEN REGEXP_LIKE(JSON_VALUE(p.respuesta, '$.data[0].FECHA_VENCIMIENTO'), '^[0-9]{8}$')
          THEN TO_CHAR(TO_DATE(JSON_VALUE(p.respuesta, '$.data[0].FECHA_VENCIMIENTO'), 'YYYYMMDD'), 'DD/MM/YYYY')
          ELSE NULL 
        END AS FECHA_VENCIMIENTO,
        JSON_VALUE(p.respuesta, '$.data[0].NOMBRE_CARRERA'    NULL ON ERROR) AS NOMBRE_CARRERA,
        JSON_VALUE(p.respuesta, '$.data[0].CARRERA'           NULL ON ERROR) AS CARRERA,
        JSON_VALUE(p.respuesta, '$.data[0].TIPO_DOCUMENTO'    NULL ON ERROR) AS TIPO_DOCUMENTO,
        REGEXP_SUBSTR(JSON_VALUE(p.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+') AS CODIGO_EXPLICATIVO,
        sap.pade_tipo_documento AS TIPO_DOCUMENTO_SAP
      FROM cuadratura_file_tbk c
      LEFT JOIN proceso_cupon p ON TO_CHAR(p.cupon) = ${cuponExpr}
      LEFT JOIN (
          SELECT 
              pa_nro_operacion, 
              MIN(pade_tipo_documento) as pade_tipo_documento -- O MAX(), ambos funcionan si el valor es siempre el mismo
          FROM pop_pagos_detalle_temp_sap
          GROUP BY pa_nro_operacion
      ) sap ON TO_CHAR(sap.pa_nro_operacion) = ${cuponExpr}
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

    const filas = (res.rows || []).map((r) => ({
      ID: r.ID,
      RUT: r.RUT,
      NOMBRE: r.NOMBRE,
      CUOTAS: r.CUOTAS,
      CUPON: r.CUPON,
      FECHA_ABONO: r.FECHA_ABONO,
      FECHA_VENTA: r.FECHA_VENTA,
      MONTO_TRANSACCION: r.MONTO_TRANSACCION,
      TIPO_TRANSACCION: r.TIPO_TRANSACCION,
      fecha_vencimiento: r.FECHA_VENCIMIENTO ?? 'No encontrado',
      nombre_carrera: r.NOMBRE_CARRERA ?? 'No encontrado',
      carrera: r.CARRERA ?? 'No encontrado',
      tipo_documento: r.TIPO_DOCUMENTO ?? 'No encontrado',
      clase_documento: r.CODIGO_EXPLICATIVO ?? 'No encontrado',
    }));

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
