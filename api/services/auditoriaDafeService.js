const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function enviarATesoreriaSoloSiSinPendientes({
  usuarioId,
  observacion,
  fecha,
  totalDiario,
  perfil,
}) {
  const conn = await getConnection();

  try {
    const binds = { fecha };

    let perfilCondition = '';
    if (perfil) {
      // --- 1. LÓGICA DE PERFIL ACTUALIZADA A LA VERSIÓN FINAL DE 3 NIVELES ---
      const expresionPerfilEfectivo = `
          CASE
              -- Prioridad 1: El centro de costo existe en la tabla de configuración SD (alias 'cfg').
              WHEN cfg.centro_cc IS NOT NULL THEN 'SD'
              
              -- Prioridad 2: El documento SAP es 'FA' (alias 'pagos').
              WHEN pagos.pade_tipo_documento = 'FA' THEN 'SD'

              -- Prioridad 3 (Caso Extremo): El campo CARRERA en el JSON es 'SD' (alias 'p').
              WHEN JSON_VALUE(p.respuesta, '$.data[0].CARRERA' NULL ON ERROR) = 'SD' THEN 'SD'
              
              -- Fallback final: Si no es SD por ninguna de las reglas anteriores, es FICA.
              ELSE 'FICA'
          END
      `;
      perfilCondition = `AND ${expresionPerfilEfectivo} = :perfil`;
      binds.perfil = perfil.toUpperCase();
    }

    const commonJoinsAndWhere = `
      FROM CUADRATURA_FILE_TBK cft
      -- Este JOIN a proceso_cupon es INNER JOIN, lo cual es correcto para esta lógica
      JOIN proceso_cupon p ON cft.ID = p.id_cuadratura
      LEFT JOIN CENTRO_CC cfg ON cft.centro_costo = cfg.centro_cc
      -- Reemplazamos el JOIN complejo por el nuevo estándar, más eficiente
      LEFT JOIN (
        SELECT pa_nro_operacion, MIN(pade_tipo_documento) AS pade_tipo_documento
        FROM pop_pagos_detalle_temp_sap
        GROUP BY pa_nro_operacion
      ) pagos ON TO_CHAR(pagos.pa_nro_operacion) = p.cupon_limpio
      WHERE cft.STATUS_SAP_REGISTER IN ('ENCONTRADO','REPROCESO','RE-PROCESADO')
      AND cft.DKTT_DT_TRAN_DAT = :fecha
      ${perfilCondition}
    `;

    const countSql = `SELECT COUNT(*) AS CANT ${commonJoinsAndWhere}`;

    const rAprob = await conn.execute(countSql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    const cant = rAprob.rows[0]?.CANT ?? 0;

    console.log(`[MODO DEPURACIÓN] Se procesarían ${cant} registros para el perfil ${perfil}.`);

    if (cant === 0) {
      const err = new Error('No hay registros aprobados para enviar en la fecha especificada.');
      err.status = 404;
      throw err;
    }

    await conn.execute(
      `INSERT INTO LOG_ENVIO_TESORERÍA (ID_AUD, DETALLE_AUDITORIA, USUARIO, REGISTROS_ENVIADOS, FECHA_ENVIO)
       VALUES (SEQ_AUD_ENVIO_TESORERIA.NEXTVAL, :totalDiario, :usuario, :cant, SYSDATE)`,
      { usuario: usuarioId, cant, totalDiario },
      { autoCommit: false }
    );

    const moverCreditosSql = `
      INSERT INTO CCN_TBK_HISTORICO (
        ID_CCN, DKTT_DT_REG, DKTT_DT_TYP, DKTT_DT_TC, DKTT_DT_SEQ_NUM,
        DKTT_DT_TRAN_DAT, DKTT_DT_TRAN_TIM, DKTT_DT_INST_RETAILER, DKTT_DT_ID_RETAILER,
        DKTT_DT_NAME_RETAILER, DKTT_DT_CARD, DKTT_DT_AMT_1, DKTT_DT_AMT_PROPINA, DKTT_TIPO_CUOTA,
        DKTT_DT_CANTI_CUOTAS, DKTT_DT_RESP_CDE, DKTT_DT_APPRV_CDE, DKTT_DT_TERM_NAME, DKTT_DT_ID_CAJA,
        DKTT_DT_NUM_BOLETA, DKTT_DT_AUTH_TRACK2, DKTT_DT_FECHA_VENTA, DKTT_DT_HORA_VENTA, DKTT_DT_FECHA_PAGO,
        DKTT_DT_COD_RECHAZO, DKTT_DT_GLOSA_RECHAZO, DKTT_DT_VAL_CUOTA, DKTT_DT_VAL_TASA, DKTT_DT_NUMERO_UNICO,
        DKTT_DT_TIPO_MONEDA, DKTT_DT_ID_RETAILER_RE,DKTT_DT_COD_SERVICIO, DKTT_DT_VCI, DKTT_MES_GRACIA,
        DKTT_PERIODO_GRACIA, TIPO_DOCUMENTO
      )
      SELECT
        cft.ID, cft.DKTT_DT_REG, cft.DKTT_DT_TYP, cft.DKTT_DT_TC,
        cft.DKTT_DT_SEQ_NUM, cft.DKTT_DT_TRAN_DAT, cft.DKTT_DT_TRAN_TIM,
        cft.DKTT_DT_INST_RETAILER, cft.DKTT_DT_ID_RETAILER,
        cft.DKTT_DT_NAME_RETAILER, cft.DKTT_DT_CARD, cft.DKTT_DT_AMT_1, cft.DKTT_DT_AMT_PROPINA,
        cft.DKTT_TIPO_CUOTA, cft.DKTT_DT_CANTI_CUOTAS,
        cft.DKTT_DT_RESP_CDE, cft.DKTT_DT_APPRV_CDE, cft.DKTT_DT_TERM_NAME,
        cft.DKTT_DT_ID_CAJA, cft.DKTT_DT_NUM_BOLETA,
        cft.DKTT_DT_AUTH_TRACK2, cft.DKTT_DT_FECHA_VENTA, cft.DKTT_DT_HORA_VENTA, cft.DKTT_DT_FECHA_PAGO,
        cft.DKTT_DT_COD_RECHAZO, cft.DKTT_DT_GLOSA_RECHAZO, cft.DKTT_DT_VAL_CUOTA,
        cft.DKTT_DT_VAL_TASA, cft.DKTT_DT_NUMERO_UNICO, cft.DKTT_DT_TIPO_MONEDA,
        cft.DKTT_DT_ID_RETAILER_RE, cft.DKTT_DT_COD_SERVICIO,
        cft.DKTT_DT_VCI, cft.DKTT_MES_GRACIA, cft.DKTT_PERIODO_GRACIA,
        REGEXP_SUBSTR(JSON_VALUE(p.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+')
      ${commonJoinsAndWhere} AND cft.TIPO_TRANSACCION = 'CCN'`;

    await conn.execute(moverCreditosSql, binds, { autoCommit: false });

    const moverDebitosSql = `
      INSERT INTO CDN_TBK_HISTORICO (
        ID_CDN, DSK_DT_REG, DSK_TYP, DSK_TC, DSK_TRAN_DAT, DSK_TRAN_TIM,
        DSK_ID_RETAILER, DSK_NAME_RETAILER, DSK_CARD, DSK_AMT_1, DSK_AMT_2,
        DSK_AMT_PROPINA, DSK_RESP_CDE, DSK_APPVR_CDE, DSK_TERMN_NAME,
        DSK_ID_CAJA, DSK_NUM_BOLETA, DSK_FECHA_PAGO, DSK_IDENT, DSK_ID_RETAILER_2,
        DSK_ID_COD_SERVI, DSK_ID_NRO_UNICO, DSK_PREPAGO, TIPO_DOCUMENTO
      )
      SELECT
        cft.ID, cft.DKTT_DT_REG, cft.DKTT_DT_TYP, cft.DKTT_DT_TC,
        cft.DKTT_DT_TRAN_DAT, cft.DKTT_DT_TRAN_TIM, cft.DKTT_DT_ID_RETAILER,
        cft.DKTT_DT_NAME_RETAILER, cft.DKTT_DT_CARD,
        cft.DKTT_DT_AMT_1, cft.DSK2_AMT_2,
        cft.DKTT_DT_AMT_PROPINA, cft.DKTT_DT_RESP_CDE, cft.DKTT_DT_APPRV_CDE,
        cft.DKTT_DT_TERM_NAME, cft.DKTT_DT_ID_CAJA, cft.DKTT_DT_NUM_BOLETA,
        cft.DKTT_DT_FECHA_VENTA, cft.DSK_IDENT,
        cft.DKTT_DT_ID_RETAILER_RE, cft.DSK_ID_COD_SERVI,
        cft.DSK_ID_NRO_UNICO, cft.DSK_PREPAGO,
        REGEXP_SUBSTR(JSON_VALUE(p.respuesta, '$.data[0].TEXTO_EXPLICATIVO' NULL ON ERROR), '[^|]+')
      ${commonJoinsAndWhere} AND cft.TIPO_TRANSACCION = 'CDN'`;

    await conn.execute(moverDebitosSql, binds, { autoCommit: false });

    const deleteSql = `DELETE FROM CUADRATURA_FILE_TBK WHERE ID IN (SELECT cft.ID ${commonJoinsAndWhere})`;

    const deleteResult = await conn.execute(deleteSql, binds, { autoCommit: false });

    await conn.commit();

    return { ok: true, cant };
  } catch (e) {
    try {
      await conn.rollback();
    } catch {}
    throw e;
  } finally {
    try {
      await conn.close();
    } catch {}
  }
}

async function existenPendientesAnterioresA({ fecha }) {
  const conn = await getConnection();
  try {
    const estadosPendientes = ['NO EXISTE', 'PENDIENTE', 'ENCONTRADO', 'REPROCESO'];

    const sql = `
      SELECT DKTT_DT_TRAN_DAT AS FECHA_MAS_RECIENTE
      FROM CUADRATURA_FILE_TBK
      WHERE DKTT_DT_TRAN_DAT < :fecha
        AND STATUS_SAP_REGISTER IN (${estadosPendientes.map((_, i) => `:estado${i}`).join(', ')})
      ORDER BY DKTT_DT_TRAN_DAT DESC
      FETCH FIRST 1 ROWS ONLY`;

    const binds = { fecha: fecha };
    estadosPendientes.forEach((estado, i) => {
      binds[`estado${i}`] = estado;
    });

    const result = await conn.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });

    if (result.rows.length > 0) {
      return { existen: true, fechaMasReciente: result.rows[0].FECHA_MAS_RECIENTE };
    } else {
      return { existen: false, fechaMasReciente: null };
    }
  } finally {
    if (conn) {
      try {
        await conn.close();
      } catch (err) {
        console.error('Error al cerrar conexión', err);
      }
    }
  }
}

module.exports = { enviarATesoreriaSoloSiSinPendientes, existenPendientesAnterioresA };
