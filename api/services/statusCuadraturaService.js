const { getConnection, exportToExcel } = require('../config/utils');
const axios = require('axios');
const iconv = require('iconv-lite');
const oracledb = require('oracledb');
require('dotenv').config(); // Para cargar las variables del .env

async function getStatusDiarioCuadratura({ fecha, perfil }) {
  const connection = await getConnection();
  const binds = { fecha }; // Objeto de binds inicial

  // 🔹 Lógica de condición dinámica según perfil con fallback
  let condicionPerfil = '';
  if (perfil) {
    const expresionPerfilEfectivo = `
        CASE
            -- Prioridad 1: El centro de costo existe en la tabla de configuración SD (alias 'cfg').
            WHEN cfg.centro_cc IS NOT NULL THEN 'SD'
            
            -- Prioridad 2: El documento SAP es 'FA' (alias 'sap').
            WHEN sap.pade_tipo_documento = 'FA' THEN 'SD'

            -- Prioridad 3 (Caso Extremo): El campo CARRERA en el JSON es 'SD' (alias 'pc').
            WHEN JSON_VALUE(pc.respuesta, '$.data[0].CARRERA' NULL ON ERROR) = 'SD' THEN 'SD'
            
            -- Fallback final: Si no es SD por ninguna de las reglas anteriores, es FICA.
            ELSE 'FICA'
        END
    `;
    condicionPerfil = `AND ${expresionPerfilEfectivo} = :perfil`;
    binds.perfil = perfil.toUpperCase();
  }

  const sqlAprobados = `
    SELECT DISTINCT
      SUM(CASE WHEN UPPER(c.STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN 1 ELSE 0 END) AS APROBADOS_DIARIO,
      SUM(CASE WHEN UPPER(c.STATUS_SAP_REGISTER) = 'ENCONTRADO' THEN TRUNC(c.DKTT_DT_AMT_1 / 100) ELSE 0 END) AS MONTO_APROBADOS
    FROM CUADRATURA_FILE_TBK c
    LEFT JOIN proceso_cupon pc ON c.id = pc.id_cuadratura
    LEFT JOIN centro_cc cfg ON c.centro_costo = cfg.centro_cc
    LEFT JOIN (
      SELECT pa_nro_operacion, MIN(pade_tipo_documento) AS pade_tipo_documento
      FROM pop_pagos_detalle_temp_sap
      GROUP BY pa_nro_operacion
    ) sap ON TO_CHAR(sap.pa_nro_operacion) = pc.cupon_limpio
    WHERE c.DKTT_DT_TRAN_DAT = :fecha
      AND TRIM(c.DKTT_DT_ID_RETAILER) NOT IN ('597048211418', '28208820', '48211418', '597028208820')
      ${condicionPerfil}
  `;

  const sqlOtros = `
    SELECT
      COUNT(*) AS TOTAL_DIARIO,
      SUM(TRUNC(DKTT_DT_AMT_1 / 100)) AS MONTO_TOTAL_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE','REPROCESO') THEN 1 ELSE 0 END) AS RECHAZADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('NO EXISTE', 'PENDIENTE','REPROCESO') THEN TRUNC(DKTT_DT_AMT_1 / 100) ELSE 0 END) AS MONTO_RECHAZADOS,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('PROCESADO') THEN 1 ELSE 0 END) AS REPROCESADOS_DIARIO,
      SUM(CASE WHEN UPPER(STATUS_SAP_REGISTER) IN ('PROCESADO') THEN TRUNC(DKTT_DT_AMT_1 / 100) ELSE 0 END) AS MONTO_REPROCESADOS
    FROM CUADRATURA_FILE_TBK
    WHERE DKTT_DT_TRAN_DAT = :fecha
      AND TRIM(DKTT_DT_ID_RETAILER) NOT IN ('597048211418', '28208820', '48211418', '597028208820')
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };

    const [resAprobados, resOtros] = await Promise.all([
      connection.execute(sqlAprobados, binds, options),
      connection.execute(sqlOtros, { fecha }, options),
    ]);

    const rAprobados = resAprobados.rows?.[0] || {};
    const rOtros = resOtros.rows?.[0] || {};

    return {
      perfil: perfil || 'TODOS',
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
    console.error('Error en getStatusDiarioCuadratura:', error);
    throw error;
  } finally {
    try {
      if (connection) await connection.close();
    } catch (err) {
      console.error('Error al cerrar la conexión:', err);
    }
  }
}

async function listarPorTipo({ fecha, estados, validarCupon = false, tipoTransaccion, perfil }) {
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
      conditions.push(`c.DKTT_DT_TRAN_DAT = :fecha`);
      binds.fecha = fecha;
    }

    /**Lógica de perfil */
    const estadosQueAplicanFiltroPerfil = ['ENCONTRADO'];
    const estadoIncluyeEncontrado =
      Array.isArray(estados) &&
      estados.some((e) => estadosQueAplicanFiltroPerfil.includes(String(e).toUpperCase()));

    if (estadoIncluyeEncontrado && perfil) {
      const expresionPerfilEfectivo = `
          CASE
              -- Prioridad 1: El centro de costo existe en la tabla de configuración SD (alias 'cfg').
              WHEN cfg.centro_cc IS NOT NULL THEN 'SD'
              
              -- Prioridad 2: El documento SAP es 'FA' (alias 'sap').
              WHEN sap.pade_tipo_documento = 'FA' THEN 'SD'

               -- Prioridad 3 (Caso Extremo): El campo CARRERA en el JSON es 'SD' (alias 'p').
              WHEN JSON_VALUE(p.respuesta, '$.data[0].CARRERA' NULL ON ERROR) = 'SD' THEN 'SD'
              
              -- Fallback final: Si no es SD por ninguna de las reglas anteriores, es FICA.
              ELSE 'FICA'
          END
      `;
      conditions.push(`${expresionPerfilEfectivo} = :perfil`);
      binds.perfil = perfil.toUpperCase();
    }

    conditions.push(
      `TRIM(c.DKTT_DT_ID_RETAILER) NOT IN ('597048211418', '28208820', '48211418', '597028208820')`
    );

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const sql = `
      SELECT
          c.id AS ID,
          MAX(p.cupon_limpio) as cupon,
          MAX(p.rut) AS RUT,
          c.NOMBRE_CLIENTE AS NOMBRE,
          TRUNC(c.DKTT_DT_AMT_1 / 100) AS MONTO_TRANSACCION,
          COALESCE(c.DKTT_DT_CANTI_CUOTAS, 0) AS CUOTAS,
          CASE
              WHEN c.tipo_transaccion IN ('CCN', 'CDN') AND REGEXP_LIKE(TRIM(c.DKTT_DT_FECHA_PAGO), '^[0-9]{6}$')
              THEN TO_CHAR(TO_DATE(TRIM(c.DKTT_DT_FECHA_PAGO), 'RRMMDD'), 'YYYYMMDD')
              ELSE NULL
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
          MAX(CASE
              WHEN REGEXP_LIKE(JSON_VALUE(p.respuesta, '$.data[0].FECHA_VENCIMIENTO'), '^[0-9]{8}$')
              THEN TO_CHAR(TO_DATE(JSON_VALUE(p.respuesta, '$.data[0].FECHA_VENCIMIENTO'), 'YYYYMMDD'), 'DD/MM/YYYY')
              ELSE NULL
          END) AS FECHA_VENCIMIENTO,
          MAX(JSON_VALUE(p.respuesta, '$.data[0].NOMBRE_CARRERA' NULL ON ERROR)) AS NOMBRE_CARRERA,
          MAX(JSON_VALUE(p.respuesta, '$.data[0].CARRERA' NULL ON ERROR)) AS CARRERA,
          MAX(JSON_VALUE(p.respuesta, '$.data[0].TIPO_DOCUMENTO' NULL ON ERROR)) AS TIPO_DOCUMENTO,
          MAX(REGEXP_SUBSTR(JSON_VALUE(p.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+')) AS CODIGO_EXPLICATIVO,
          MAX(sap.pade_tipo_documento) AS TIPO_DOCUMENTO_SAP,
          c.STATUS_SAP_REGISTER AS ESTADO
      FROM
          cuadratura_file_tbk c
      LEFT JOIN proceso_cupon p ON c.id = p.id_cuadratura 
      LEFT JOIN (
          SELECT
              pa_nro_operacion,
              MIN(pade_tipo_documento) AS pade_tipo_documento
          FROM
              pop_pagos_detalle_temp_sap
          GROUP BY
              pa_nro_operacion
      ) sap ON TO_CHAR(sap.pa_nro_operacion) = p.cupon_limpio
      LEFT JOIN
          CENTRO_CC cfg ON c.centro_costo = cfg.centro_cc
      ${whereClause}
      GROUP BY
          c.id,
          c.NOMBRE_CLIENTE,
          c.DKTT_DT_AMT_1,
          c.DKTT_DT_CANTI_CUOTAS,
          c.tipo_transaccion,
          c.DKTT_DT_FECHA_PAGO,
          c.DKTT_DT_TRAN_DAT,
          c.STATUS_SAP_REGISTER
    `;

    const res = await conn.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });

    const filasLimpias = (res.rows || []).map((fila) => {
      const { TIPO_DOCUMENTO_SAP, ...filaParaRetornar } = fila;

      const nombreCarreraCorregido = filaParaRetornar.NOMBRE_CARRERA
        ? Buffer.from(filaParaRetornar.NOMBRE_CARRERA, 'latin1').toString('utf8').trim()
        : 'No encontrado';

      return {
        ...filaParaRetornar,
        NOMBRE_CARRERA: nombreCarreraCorregido,
      };
    });

    return filasLimpias;
  } finally {
    try {
      await conn.close();
    } catch {}
  }
}

async function generarExcelReporteCompleto(params, res) {
  const { fecha, perfil } = params;
  const conn = await getConnection();

  try {
    const binds = {};
    const conditions = [];

    // 1. Condición de Fecha (se mantiene)
    if (fecha) {
      conditions.push(`c.DKTT_DT_TRAN_DAT = :fecha`);
      binds.fecha = fecha;
    }

    // 2. Lógica de Perfil (se mantiene, pero sin depender de 'estados')
    // AHORA SE APLICA SIEMPRE QUE SE ENVÍE EL PARÁMETRO 'perfil'
    if (perfil) {
      const expresionPerfilEfectivo = `
          CASE
              WHEN cfg.centro_cc IS NOT NULL THEN 'SD'
              WHEN sap.pade_tipo_documento = 'FA' THEN 'SD'
              WHEN JSON_VALUE(p.respuesta, '$.data[0].CARRERA' NULL ON ERROR) = 'SD' THEN 'SD'
              ELSE 'FICA'
          END
      `;
      conditions.push(`${expresionPerfilEfectivo} = :perfil`);
      binds.perfil = perfil.toUpperCase();
    }

    // 3. Condición de Retailers Excluidos (se mantiene)
    conditions.push(
      `TRIM(c.DKTT_DT_ID_RETAILER) NOT IN ('597048211418', '28208820', '48211418', '597028208820')`
    );

    const whereClause = `WHERE ${conditions.join(' AND ')}`;

    const sql = `
      SELECT DISTINCT
        c.id AS ID,
        p.cupon_limpio as cupon, 
        p.rut AS RUT,
        c.NOMBRE_CLIENTE AS NOMBRE,
        TRUNC(c.DKTT_DT_AMT_1 / 100) AS MONTO_TRANSACCION,
        COALESCE(c.DKTT_DT_CANTI_CUOTAS, 0) AS CUOTAS,
        CASE
            WHEN c.tipo_transaccion IN ('CCN', 'CDN') AND REGEXP_LIKE(TRIM(c.DKTT_DT_FECHA_PAGO), '^[0-9]{6}$')
            THEN TO_CHAR(TO_DATE(TRIM(c.DKTT_DT_FECHA_PAGO), 'RRMMDD'), 'YYYYMMDD')
            ELSE NULL
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
        JSON_VALUE(p.respuesta, '$.data[0].NOMBRE_CARRERA' NULL ON ERROR) AS NOMBRE_CARRERA,
        JSON_VALUE(p.respuesta, '$.data[0].CARRERA' NULL ON ERROR) AS CARRERA,
        JSON_VALUE(p.respuesta, '$.data[0].TIPO_DOCUMENTO' NULL ON ERROR) AS TIPO_DOCUMENTO,
        REGEXP_SUBSTR(JSON_VALUE(p.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+') AS CODIGO_EXPLICATIVO,
        sap.pade_tipo_documento AS TIPO_DOCUMENTO_SAP
      FROM
          cuadratura_file_tbk c
      LEFT JOIN proceso_cupon p ON c.id = p.id_cuadratura 
      LEFT JOIN (
          SELECT pa_nro_operacion, MIN(pade_tipo_documento) AS pade_tipo_documento
          FROM pop_pagos_detalle_temp_sap
          GROUP BY pa_nro_operacion
      ) sap ON TO_CHAR(sap.pa_nro_operacion) = p.cupon_limpio
      LEFT JOIN
          CENTRO_CC cfg ON c.centro_costo = cfg.centro_cc
      ${whereClause}
    `;

    const result = await conn.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });

    const filasLimpias = (result.rows || []).map((fila) => ({
      ...fila,
      NOMBRE_CARRERA: fila.NOMBRE_CARRERA
        ? Buffer.from(fila.NOMBRE_CARRERA, 'latin1').toString('utf8').trim()
        : 'No encontrado',
    }));

    if (filasLimpias.length === 0) {
      res.status(404).json({
        success: false,
        message: 'No se encontraron registros para los filtros seleccionados.',
      });
      return;
    }

    const nombreArchivo = `Reporte_Completo_${fecha}.xlsx`;
    await exportToExcel(filasLimpias, res, nombreArchivo);
  } finally {
    try {
      await conn.close();
    } catch {}
  }
}

module.exports = {
  getStatusDiarioCuadratura,
  listarPorTipo,
  generarExcelReporteCompleto,
};
