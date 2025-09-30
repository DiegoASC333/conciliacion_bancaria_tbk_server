const { getConnection, exportToExcel } = require('../config/utils');
const { buildLiquidacionQuery } = require('../config/queryBuilder');
const oracledb = require('oracledb');

async function getLiquidacion({ tipo, startLCN, startLDN }) {
  const connection = await getConnection();
  try {
    const { sql, binds } = buildLiquidacionQuery({ tipo, startLCN, startLDN });
    const res = await connection.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return res.rows || [];
  } finally {
    try {
      await connection.close();
    } catch {}
  }
}

async function getLiquidacionTotales({ tipo, startLCN, startLDN }) {
  const connection = await getConnection();

  const tipoUpper = (tipo || '').toUpperCase();

  let where = '';
  let binds = {};

  if (tipoUpper === 'LCN') {
    where = "TIPO_TRANSACCION = 'LCN' AND liq_fpago = :fecha";
    binds.fecha = startLCN;
  } else if (tipoUpper === 'LDN') {
    where = "TIPO_TRANSACCION = 'LDN' AND liq_fedi = :fecha";
    binds.fecha = startLDN;
  }

  let comercioExpr;
  if (tipoUpper === 'LCN') {
    comercioExpr = `CASE WHEN l.liq_cprin != 99999999 THEN TRIM(l.liq_cprin) ELSE TRIM(l.liq_numc) END`;
  } else {
    comercioExpr = `TRIM(l.liq_numc)`;
  }

  const sql = `
    SELECT
      ${comercioExpr}                           AS CODIGO_COMERCIO,
      c.NOMBRE_COMERCIO AS NOMBRE_COMERCIO,
      c.CUENTA_CORRIENTE AS CUENTA_CORRIENTE,
      c.BANCO AS  BANCO,
      c.CUENTA_CONTABLE AS CUENTA_CONTABLE,
      c.DESCRIPCION AS DESCRIPCION,
      SUM(l.liq_monto / 100)                     AS TOTAL_MONTO
      FROM liquidacion_file_tbk l
      LEFT JOIN vec_cob04.codigo_comerico c
    ON c.codigo_comerico = ${comercioExpr}
    WHERE ${where}
    AND ${comercioExpr} NOT IN ('28208820', '48211418')
    GROUP BY ${comercioExpr}, c.NOMBRE_COMERCIO, c.CUENTA_CORRIENTE, c.BANCO, c.CUENTA_CONTABLE, c.DESCRIPCION
    ORDER BY TOTAL_MONTO DESC
  `;

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    //const binds = { tipo: tipoUpper, startLCN, startLDN };
    const res = await connection.execute(sql, binds, options);
    return res.rows || [];
  } catch (error) {
    console.error('Error al obtener totales de liquidación:', error);
    throw error;
  } finally {
    try {
      await connection.close();
    } catch (_) {}
  }
}

async function findLatestPendingDate({ tipo, fecha }) {
  const connection = await getConnection();
  const tipoUpper = (tipo || '').toUpperCase();

  let sql;

  if (tipoUpper === 'LCN') {
    sql = `
      SELECT MAX(TO_DATE(liq_fpago, 'DDMMYYYY')) AS LATEST_DATE
      FROM liquidacion_file_tbk
      WHERE TIPO_TRANSACCION = 'LCN'
        AND REGEXP_LIKE(TRIM(liq_fpago), '^[0-9]{8}$')
        AND TO_DATE(liq_fpago, 'DDMMYYYY') < TO_DATE(:fecha, 'YYYY-MM-DD')
    `;
  } else if (tipoUpper === 'LDN') {
    sql = `
      SELECT MAX(TO_DATE(liq_fedi, 'DD/MM/RR')) AS LATEST_DATE
      FROM liquidacion_file_tbk
      WHERE TIPO_TRANSACCION = 'LDN'
        AND REGEXP_LIKE(TRIM(liq_fedi), '^[0-9]{2}/[0-9]{2}/[0-9]{2}$')
        AND TO_DATE(liq_fedi, 'DD/MM/RR') < TO_DATE(:fecha, 'YYYY-MM-DD')
    `;
  } else {
    return null;
  }

  try {
    const res = await connection.execute(sql, { fecha }, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    if (res.rows && res.rows.length > 0) {
      return res.rows[0].LATEST_DATE;
    }
    return null;
  } finally {
    try {
      await connection.close();
    } catch {}
  }
}

async function getLiquidacionExcel({ tipo, startLCN, startLDN }, res) {
  let connection;

  try {
    connection = await getConnection();

    const { sql, binds } = buildLiquidacionQuery({ tipo, startLCN, startLDN });

    const result = await connection.execute(sql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    await exportToExcel(result.rows, res, `liquidaciones_${tipo}.xlsx`);
  } catch (err) {
    console.error('Error exportando Excel:', err);
    res.status(500).json({ error: 'Error al exportar Excel' });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error('Error cerrando conexión:', err);
      }
    }
  }
}

async function guardarLiquidacionesHistoricas({ tipo, fecha, usuarioId }) {
  const connection = await getConnection();
  const tipoUpper = tipo.toUpperCase();

  const [yyyy, mm, dd] = fecha.split('-');
  const fechaPago_LCN = `${dd}${mm}${yyyy}`;
  const fechaEdi_LDN = `${dd}/${mm}/${yyyy.slice(2)}`;

  let whereClause = '';
  let binds = {};

  const isValid = (col) => `REGEXP_LIKE(TRIM(l.${col}), '^[0-9]*[1-9][0-9]*$')`;

  if (tipoUpper === 'LCN') {
    whereClause = `
      WHERE l.TIPO_TRANSACCION = :tipo 
      AND l.liq_fpago = :fechaParam
      AND EXISTS (
          SELECT 1 FROM CCN_TBK_HISTORICO h
          WHERE 
            (${isValid('liq_orpedi')} AND LTRIM(TRIM(l.liq_orpedi), '0') = LTRIM(TRIM(h.DKTT_DT_NUMERO_UNICO), '0'))
            OR 
            (NOT ${isValid('liq_orpedi')} AND TRIM(l.liq_codaut) = h.DKTT_DT_APPRV_CDE)
      )`;
    binds = { tipo: tipoUpper, fechaParam: fechaPago_LCN };
  } else if (tipoUpper === 'LDN') {
    whereClause = `
      WHERE l.TIPO_TRANSACCION = :tipo
      AND l.liq_fedi = :fechaParam
      AND EXISTS (
          SELECT 1 FROM CDN_TBK_HISTORICO h
          WHERE 
            (${isValid('liq_nro_unico')} AND LTRIM(TRIM(l.liq_nro_unico), '0') = LTRIM(TRIM(h.DSK_ID_NRO_UNICO), '0'))
            OR 
            (NOT ${isValid('liq_nro_unico')} AND TRIM(l.liq_appr) = h.DSK_APPVR_CDE)
      )`;
    binds = { tipo: tipoUpper, fechaParam: fechaEdi_LDN };
  } else {
    throw new Error('Tipo no soportado');
  }

  try {
    const countSql = `SELECT COUNT(*) AS CANT FROM LIQUIDACION_FILE_TBK l ${whereClause}`;
    const countResult = await connection.execute(countSql, binds, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });
    const cant = countResult.rows[0].CANT ?? 0;

    if (cant === 0) {
      throw {
        status: 404,
        message: 'No hay liquidaciones validadas para procesar en la fecha especificada.',
      };
    }

    await connection.execute(
      `INSERT INTO LOG_ENVIO_CONTABILIDAD (ID_AUD, USUARIO, REGISTROS_ENVIADOS, FECHA_ENVIO)
       VALUES (SEQ_AUD_ENVIO_CONTABILIDAD.NEXTVAL, :usuarioId, :cant, SYSDATE)`,
      { usuarioId, cant },
      { autoCommit: false }
    );

    let moverDatosSql = '';

    if (tipoUpper === 'LCN') {
      moverDatosSql = `
    INSERT INTO LCN_TBK_HISTORICO (
      ID_LCN, LIQ_NUMC, LIQ_FPROC, LIQ_FCOM, LIQ_MICR, LIQ_NUMTA, LIQ_MARCA,
      LIQ_MONTO, LIQ_MONEDA, LIQ_TXS, LIQ_RETE, LIQ_CPRIN, LIQ_FPAGO, LIQ_ORPEDI,
      LIQ_CODAUT, LIQ_CUOTAS, LIQ_VCI, LIQ_CEIC, LIQ_CAEICA, LIQ_DEIC, 
      LIQ_DCAEICA, LIQ_NTC, LIQ_NOMBRE_BANCO, LIQ_TIPO_CUENTA_BANCO, 
      LIQ_NUMERO_CUENTA_BANCO, LIQ_MONEDA_CUENTA_BANCO
    ) 
    SELECT
      l.ID, l.LIQ_NUMC, l.LIQ_FPROC, l.LIQ_FCOM, l.LIQ_MICR, l.LIQ_NUMTA, l.LIQ_MARCA,
      l.LIQ_MONTO, l.LIQ_MONEDA, l.LIQ_TXS, l.LIQ_RETE, l.LIQ_CPRIN, l.LIQ_FPAGO, l.LIQ_ORPEDI,
      l.LIQ_CODAUT, l.LIQ_CUOTAS, l.LIQ_VCI, l.LIQ_CEIC, l.LIQ_CAEICA, l.LIQ_DEIC,
      l.LIQ_DCAEICA, l.LIQ_NTC, l.LIQ_NOMBRE_BANCO, l.LIQ_TIPO_CUENTA_BANCO,
      l.LIQ_NUMERO_CUENTA_BANCO, l.LIQ_MONEDA_CUENTA_BANCO
    FROM
      LIQUIDACION_FILE_TBK l
    ${whereClause}
  `;
    } else if (tipoUpper === 'LDN') {
      moverDatosSql = `
    INSERT INTO LDN_TBK_HISTORICO (
      ID_LDN, LIQ_CCRE, LIQ_FPRO, LIQ_FCOM, LIQ_APPR, LIQ_PAN, LIQ_AMT_1, LIQ_TTRA, 
      LIQ_CPRI, LIQ_MARC, LIQ_FEDI, LIQ_NRO_UNICO, LIQ_COM_COMIV, LIQ_CAD_CADIVA, 
      LIQ_DECOM_IVCOM, LIQ_DCOAD_IVCOM, LIQ_PREPAGO, LIQ_NOMBRE_BANCO, LIQ_TIPO_CUENTA_BANCO, 
      LIQ_NUMERO_CUENTA_BANCO, LIQ_MONEDA_CUENTA_BANCO
    )
    SELECT
      l.ID, l.LIQ_CCRE, l.LIQ_FPRO, l.LIQ_FCOM, l.LIQ_APPR, l.LIQ_PAN, l.LIQ_AMT_1, l.LIQ_TTRA, 
      l.LIQ_CPRI, l.LIQ_MARC, l.LIQ_FEDI, l.LIQ_NRO_UNICO, l.LIQ_COM_COMIV, l.LIQ_CAD_CADIVA, 
      l.LIQ_DECOM_IVCOM, l.LIQ_DCOAD_IVCOM, l.LIQ_PREPAGO, l.LIQ_NOMBRE_BANCO, l.LIQ_TIPO_CUENTA_BANCO, 
      l.LIQ_NUMERO_CUENTA_BANCO, l.LIQ_MONEDA_CUENTA_BANCO 
    FROM
      LIQUIDACION_FILE_TBK l
    ${whereClause}
  `;
    }

    const moveResult = await connection.execute(moverDatosSql, binds, { autoCommit: false });

    const deleteSql = `DELETE FROM LIQUIDACION_FILE_TBK l ${whereClause}`;
    await connection.execute(deleteSql, binds, { autoCommit: false });

    await connection.commit();
    return { ok: true, registrosProcesados: moveResult.rowsAffected };
  } catch (e) {
    await connection.rollback();
    console.error('Error en la transacción de validación, se hizo rollback:', e);
    if (e.status) throw e;
    throw new Error('Error interno al procesar la validación.');
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}

module.exports = {
  getLiquidacion,
  getLiquidacionTotales,
  getLiquidacionExcel,
  guardarLiquidacionesHistoricas,
  findLatestPendingDate,
};
