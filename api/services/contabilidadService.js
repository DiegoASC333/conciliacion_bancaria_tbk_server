const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');
const ExcelJS = require('exceljs');

function parsearFecha(fechaStr) {
  const [dia, mes, anio] = fechaStr.split('/');
  // Los meses en JavaScript son 0-indexados (Enero=0, Diciembre=11)
  return new Date(anio, mes - 1, dia);
}

async function getDataDescargaExcel({ fecha_inicio, fecha_fin }) {
  const connection = await getConnection();

  const fechaInicioObj = parsearFecha(fecha_inicio);
  const fechaFinObj = parsearFecha(fecha_fin);

  fechaFinObj.setHours(23, 59, 59);

  const sqlQuery = `
    SELECT
      TO_DATE(l.liq_fedi, 'DD/MM/RR') AS FECHA_REAL, 'DEBITO' AS TIPO, l.liq_fedi AS FECHA_TEXTO,
      SUM(l.liq_amt_1 / 100) AS TOTAL_MONTO, l.LIQ_CCRE AS CODIGO_COMERCIO, c.NOMBRE_COMERCIO,
      c.CUENTA_CORRIENTE, c.BANCO, c.CUENTA_CONTABLE, c.DESCRIPCION
    FROM ldn_tbk_historico l
    LEFT JOIN vec_cob04.codigo_comerico c ON c.codigo_comerico = l.LIQ_CCRE
    WHERE
      TO_DATE(l.liq_fedi, 'DD/MM/RR') BETWEEN :startDate AND :endDate
      AND l.LIQ_CCRE NOT IN ('28208820', '48211418', '41246590', '41246593', '41246594')
    GROUP BY
      l.liq_fedi, l.LIQ_CCRE, c.NOMBRE_COMERCIO, c.CUENTA_CORRIENTE, c.BANCO, c.CUENTA_CONTABLE, c.DESCRIPCION

    UNION ALL

    SELECT
      TO_DATE(liq_fpago, 'DDMMYYYY') AS FECHA_REAL, 'CREDITO' AS TIPO, liq_fpago AS FECHA_TEXTO,
      SUM(TRUNC(liq_monto / 100)) AS TOTAL_MONTO, NULL AS CODIGO_COMERCIO, NULL AS NOMBRE_COMERCIO,
      NULL AS CUENTA_CORRIENTE, NULL AS BANCO, NULL AS CUENTA_CONTABLE, NULL AS DESCRIPCION
    FROM lcn_tbk_historico
    WHERE
      TO_DATE(liq_fpago, 'DDMMYYYY') BETWEEN :startDate AND :endDate
    GROUP BY
      liq_fpago

    ORDER BY FECHA_REAL, TIPO, TOTAL_MONTO DESC
  `;

  const params = {
    startDate: fechaInicioObj,
    endDate: fechaFinObj,
  };

  try {
    const options = { outFormat: oracledb.OUT_FORMAT_OBJECT };
    const res = await connection.execute(sqlQuery, params, options);
    return res.rows || [];
  } catch (err) {
    console.error('Error en el servicio al ejecutar la consulta:', err);
    throw err; // Lanza el error para que el controlador lo capture
  } finally {
    if (connection) {
      try {
        await connection.close(); // Siempre cierra la conexión
      } catch (err) {
        console.error('Error al cerrar la conexión:', err);
      }
    }
  }
}

async function getDataExcelPorDia({ fecha }) {
  const connection = await getConnection();
  try {
    // La consulta se define con una única cláusula WITH al inicio
    const sql = `
      WITH Clases_Documento_Unicas_C AS (
          SELECT
              l.liq_monto, l.liq_orpedi, l.liq_codaut, l.liq_fcom,
              TO_DATE(l.liq_fpago, 'DDMMYYYY') AS FECHA_REAL, 'CREDITO' AS TIPO, NULL AS FECHA_TEXTO, NULL AS CODIGO_COMERCIO,
              NULL AS NOMBRE_COMERCIO, NULL AS CUENTA_CORRIENTE, NULL AS BANCO, '1101050021' AS CUENTA_CONTABLE_COMERCIO, NULL AS DESCRIPCION,
              NVL(h.tipo_documento, 'Z5') AS clase_doc_final, 
              ROW_NUMBER() OVER (PARTITION BY l.liq_orpedi, l.liq_codaut, l.liq_fcom ORDER BY h.tipo_documento DESC) AS rn
          FROM
              lcn_tbk_historico l
          LEFT JOIN 
              CCN_TBK_HISTORICO h ON
              (
                  REGEXP_LIKE(TRIM(l.liq_orpedi), '^[0-9]*[1-9][0-9]*$') AND LTRIM(TRIM(l.liq_orpedi), '0') = LTRIM(TRIM(h.DKTT_DT_NUMERO_UNICO), '0')
              ) OR (
                  NOT REGEXP_LIKE(TRIM(l.liq_orpedi), '^[0-9]*[1-9][0-9]*$') AND TRIM(l.liq_codaut) = h.DKTT_DT_APPRV_CDE
              )
              
              -- --- AJUSTE ORA-01858 (CRÉDITO): Protege TO_DATE de valores no numéricos en la unión.
              AND TO_DATE(
                  CASE WHEN REGEXP_LIKE(TO_CHAR(l.liq_fcom), '^[0-9]{7,8}$') THEN LPAD(TO_CHAR(l.liq_fcom), 8, '0') ELSE NULL END, 
                  'DDMMYYYY'
              ) = TO_DATE(
                  CASE WHEN REGEXP_LIKE(TO_CHAR(h.DKTT_DT_TRAN_DAT), '^[0-9]{5,6}$') THEN LPAD(TO_CHAR(h.DKTT_DT_TRAN_DAT), 6, '0') ELSE NULL END, 
                  'RRMMDD'
              )
              
          WHERE
              -- --- AJUSTE FILTRO (CRÉDITO): Asegura la igualdad de fechas a nivel de día.
              TRUNC(TO_DATE(l.liq_fpago, 'DDMMYYYY')) = TRUNC(TO_DATE(:fecha, 'DD/MM/YYYY'))
      ),
      Consolidacion_Final_C AS (
          SELECT
              cdu.FECHA_REAL, cdu.TIPO, cdu.FECHA_TEXTO, cdu.CODIGO_COMERCIO, cdu.NOMBRE_COMERCIO, 
              cdu.CUENTA_CORRIENTE, cdu.BANCO, cdu.CUENTA_CONTABLE_COMERCIO, cdu.DESCRIPCION,
              CASE WHEN cdu.clase_doc_final = 'ZC' THEN 'ZB' ELSE cdu.clase_doc_final END AS clase_consolidada,
              cdu.liq_monto
          FROM Clases_Documento_Unicas_C cdu
          WHERE cdu.rn = 1
      ),
      Clases_Documento_Unicas_D AS (
          SELECT
              l.liq_amt_1, l.liq_nro_unico, l.liq_appr, l.liq_fcom,
              TO_DATE(l.liq_fedi, 'DD/MM/RR') AS FECHA_REAL, 'DEBITO' AS TIPO, l.liq_fedi AS FECHA_TEXTO, l.liq_ccre AS CODIGO_COMERCIO_ID,
              NVL(h.tipo_documento, 'Z5') AS clase_doc_final, 
              ROW_NUMBER() OVER (PARTITION BY l.liq_nro_unico, l.liq_appr, l.liq_fcom, l.liq_amt_1 ORDER BY h.tipo_documento DESC) AS rn
          FROM
              ldn_tbk_historico l
          LEFT JOIN CDN_TBK_HISTORICO h ON
              (
                  REGEXP_LIKE(TRIM(l.liq_nro_unico), '^[0-9]*[1-9][0-9]*$') AND LTRIM(TRIM(l.liq_nro_unico), '0') = LTRIM(TRIM(h.DSK_ID_NRO_UNICO), '0')
              ) OR (
                  NOT REGEXP_LIKE(TRIM(l.liq_nro_unico), '^[0-9]*[1-9][0-9]*$') AND TRIM(l.liq_appr) = h.DSK_APPVR_CDE
              )
              
              -- --- AJUSTE ORA-01858 (DÉBITO): Protege TO_DATE de valores no numéricos en la unión.
              AND TO_DATE(
                  CASE WHEN REGEXP_LIKE(TO_CHAR(l.liq_fcom), '^[0-9]{5,6}$') THEN LPAD(TO_CHAR(l.liq_fcom), 6, '0') ELSE NULL END, 
                  'DDMMRR'
              ) = TO_DATE(
                  CASE WHEN REGEXP_LIKE(TO_CHAR(h.DSK_TRAN_DAT), '^[0-9]{5,6}$') THEN LPAD(TO_CHAR(h.DSK_TRAN_DAT), 6, '0') ELSE NULL END, 
                  'RRMMDD'
              )
              AND l.LIQ_AMT_1 = h.DSK_AMT_1
          WHERE
              -- --- AJUSTE FILTRO (DÉBITO): Asegura la igualdad de fechas a nivel de día.
              TRUNC(TO_DATE(l.liq_fedi, 'DD/MM/RR')) = TRUNC(TO_DATE(:fecha, 'DD/MM/YYYY')) 
              AND l.liq_ccre NOT IN ( '28208820', '48211418', '41246590', '41246593', '41246594' )
      ),
      Consolidacion_Final_D AS (
          SELECT
              cdu.FECHA_REAL, cdu.TIPO, cdu.FECHA_TEXTO, cdu.CODIGO_COMERCIO_ID, 
              CASE WHEN cdu.clase_doc_final IN ('ZA', 'ZC') THEN 'ZA' ELSE cdu.clase_doc_final END AS clase_consolidada,
              cdu.liq_amt_1
          FROM Clases_Documento_Unicas_D cdu
          WHERE cdu.rn = 1
      ),
      Cuenta_Factores AS (
          SELECT clase_documento_sap AS clase_consolidada, COUNT(DISTINCT CUENTA_CONTABLE) AS factor_multiplicador
          FROM vec_cob02.tipo_pago_descripcion
          GROUP BY clase_documento_sap
      ),
      Suma_Consolidada_C AS (
          SELECT clase_consolidada, SUM(TRUNC(liq_monto / 100)) AS monto_total_correcto
          FROM Consolidacion_Final_C
          GROUP BY clase_consolidada
      ),
      Suma_Consolidada_D AS (
          SELECT cf.clase_consolidada, cf.CODIGO_COMERCIO_ID, SUM(cf.liq_amt_1 / 100) AS monto_total_correcto
          FROM Consolidacion_Final_D cf
          GROUP BY cf.clase_consolidada, cf.CODIGO_COMERCIO_ID
      )
      -- RAMA 1: CRÉDITO
      SELECT
          cf.clase_consolidada AS CLASE_DOCUMENTO, cf.FECHA_REAL, cf.TIPO, cf.FECHA_TEXTO,
          sc.monto_total_correcto / NVL(af.factor_multiplicador, 1) AS TOTAL_MONTO, 
          cf.CODIGO_COMERCIO, cf.NOMBRE_COMERCIO, cf.CUENTA_CORRIENTE, cf.BANCO, 
          cf.CUENTA_CONTABLE_COMERCIO, cf.DESCRIPCION, tpd_za.CUENTA_CONTABLE AS CUENTA_TRANSACCION
      FROM
          Consolidacion_Final_C cf
      LEFT JOIN vec_cob02.tipo_pago_descripcion tpd_za 
          ON tpd_za.clase_documento_sap = cf.clase_consolidada
      LEFT JOIN Cuenta_Factores af
          ON af.clase_consolidada = cf.clase_consolidada
      LEFT JOIN Suma_Consolidada_C sc
          ON sc.clase_consolidada = cf.clase_consolidada
      GROUP BY
          cf.clase_consolidada, cf.FECHA_REAL, cf.TIPO, cf.FECHA_TEXTO,
          sc.monto_total_correcto / NVL(af.factor_multiplicador, 1),
          cf.CODIGO_COMERCIO, cf.NOMBRE_COMERCIO, cf.CUENTA_CORRIENTE, cf.BANCO, 
          cf.CUENTA_CONTABLE_COMERCIO, cf.DESCRIPCION, tpd_za.CUENTA_CONTABLE

      UNION ALL

      -- RAMA 2: DÉBITO
      SELECT
          cf.clase_consolidada AS CLASE_DOCUMENTO, cf.FECHA_REAL, cf.TIPO, cf.FECHA_TEXTO,
          sc.monto_total_correcto / NVL(af.factor_multiplicador, 1) AS TOTAL_MONTO,
          cf.CODIGO_COMERCIO_ID AS CODIGO_COMERCIO, c.nombre_comercio AS NOMBRE_COMERCIO,
          c.cuenta_corriente AS CUENTA_CORRIENTE, c.banco AS BANCO, 
          c.cuenta_contable AS CUENTA_CONTABLE_COMERCIO, c.descripcion AS DESCRIPCION,
          tpd_za.CUENTA_CONTABLE AS CUENTA_TRANSACCION
      FROM
          Consolidacion_Final_D cf
      LEFT JOIN vec_cob04.codigo_comerico c 
          ON c.codigo_comerico = cf.CODIGO_COMERCIO_ID
      LEFT JOIN vec_cob02.tipo_pago_descripcion tpd_za
          ON tpd_za.clase_documento_sap = cf.clase_consolidada
      LEFT JOIN Cuenta_Factores af
          ON af.clase_consolidada = cf.clase_consolidada
      LEFT JOIN Suma_Consolidada_D sc
          ON sc.clase_consolidada = cf.clase_consolidada AND sc.CODIGO_COMERCIO_ID = cf.CODIGO_COMERCIO_ID
      GROUP BY
          cf.clase_consolidada, cf.FECHA_REAL, cf.TIPO, cf.FECHA_TEXTO,
          sc.monto_total_correcto / NVL(af.factor_multiplicador, 1),
          cf.CODIGO_COMERCIO_ID, c.nombre_comercio, c.cuenta_corriente, c.banco, 
          c.cuenta_contable, c.descripcion, tpd_za.CUENTA_CONTABLE
      ORDER BY
          FECHA_REAL, CLASE_DOCUMENTO
    `;

    const res = await connection.execute(sql, { fecha }, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return res.rows || [];
  } finally {
    try {
      await connection.close();
    } catch (err) {
      console.error('Error closing connection:', err);
      // Opcional: manejar el error de cierre, aunque generalmente se ignora.
    }
  }
}

async function generarReporte(dataRows, fechaConsulta) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet('Reporte SAP');

  const headerRow1 = [
    'DOC',
    'BLDAT',
    'BLART',
    'BUKRS',
    'BUDAT',
    'MONAT',
    'WAERS',
    'XBLNR',
    'BKTXT',
    'DOCID',
    'NEWBS',
    'NEWKO',
    'WRBTR',
    'VALUT',
    'SGTXT',
    'NEWBS_01',
    'NEWKO_01',
    'WRBTR_01',
    'SGTXT_01',
  ];
  const headerRow2 = [
    'DOC',
    '10.01.1900',
    2,
    4,
    '10',
    2,
    5,
    16,
    25,
    10,
    2,
    17,
    16,
    10,
    50,
    2,
    17,
    16,
    50,
  ];
  const headerRow3 = [
    'DOC',
    'Fecha de documento en documento',
    'Clase de documento',
    'Sociedad',
    'Fecha de contabilización en el documento',
    'Mes contable',
    'Clave de moneda',
    'Número de documento de referencia',
    'Texto de cabecera de documento',
    'Clase documentos',
    'Clave de contabilización para la siguiente posición',
    'Cuenta banco',
    'Importe en la moneda del documento',
    'Fecha valor',
    'Texto posición',
    'Clave de contabilización para la siguiente posición',
    'Cuenta o matchcode para la siguiente posición',
    'Importe en la moneda del documento',
    'Texto posición',
  ];

  worksheet.addRow(headerRow1);
  worksheet.addRow(headerRow2);
  worksheet.addRow(headerRow3);

  worksheet.columns = [
    { key: 'DOC', width: 8 }, // Col A
    { key: 'BLDAT', width: 12 }, // Col B - Fecha
    { key: 'BLART', width: 10 }, // Col C
    { key: 'BURSK', width: 10 }, // Col D
    { key: 'BUDAT', width: 12 }, // Col E - Fecha
    { key: 'MONAT', width: 8 }, // Col F - Mes
    { key: 'WAERS', width: 8 }, // Col G - Moneda
    { key: 'XBLNR', width: 15 }, // Col H - 'TRANSBANK'
    { key: 'BKTXT', width: 15 }, // Col I - 'CARTOLA 1'
    { key: 'DOCID', width: 8 }, // Col J
    { key: 'NEWBS', width: 8 }, // Col K
    { key: 'NEWKO', width: 15 }, // Col L - Cuenta banco
    { key: 'WRBTR', width: 18 }, // Col M - Importe
    { key: 'VALUT', width: 12 }, // Col N - Fecha
    { key: 'SGTXT', width: 50 }, // Col O - Texto (¡La más larga!)
    { key: 'NEWBS_01', width: 8 }, // Col P
    { key: 'NEWKO_01', width: 18 }, // Col Q - Cuenta matchcode
    { key: 'WRBTR_01', width: 18 }, // Col R - Importe
    { key: 'SGTXT_01', width: 50 }, // Col S - Texto (¡La más larga!)
  ];

  [1, 2, 3].forEach((rowNumber) => {
    worksheet.getRow(rowNumber).font = { bold: true };
  });

  worksheet.getColumn('B').numFmt = 'dd.mm.yyyy'; // BLDAT
  worksheet.getColumn('E').numFmt = 'dd.mm.yyyy'; // BUDAT
  worksheet.getColumn('N').numFmt = 'dd.mm.yyyy'; // VALUT

  worksheet.getColumn('M').numFmt = '#,##0'; // WRBTR
  worksheet.getColumn('R').numFmt = '#,##0'; // WRBTR_01

  const mesContableNum = fechaConsulta.getMonth() + 1;
  const mesContableStr = mesContableNum.toString().padStart(2, '0');
  const anioConsulta = fechaConsulta.getFullYear();
  const textoPosicion = `ABONO EN CTA CTE. RECAUDACION TRANSBANK ${mesContableStr}${anioConsulta}`;

  dataRows.forEach((row, index) => {
    //const incremental = index + 1;

    const fechaFormateada = formatFechaSAP(row.FECHA_REAL);
    const montoFormateado = formatMonto(row.TOTAL_MONTO);

    const excelRow = [
      row.CLASE_DOCUMENTO, // 1. DOC (Antes era row.DOC)
      fechaFormateada, // 2. BLDAT (Fecha doc)
      'CB', // 3. BLART (Fijo)
      'UT01', // 4. BURSK (Fijo de headerRow2)
      fechaFormateada, // 5. BUDAT (Fecha contab)
      mesContableNum, // 6. MONAT (Calculado)
      'CLP', // 7. WAERS (Fijo)
      'TRANSBANK', // 8. XBLNR (Fijo)
      `CARTOLA 01`, // 9. BKTXT (Calculado)
      '*/', // 10. DOCID (Fijo)
      '40', // 11. NEWBS (Fijo)
      //row.CUENTA_CORRIENTE, // 12. NEWKO (Dato de la query, será null para 'ZB')
      row.CUENTA_CONTABLE_COMERCIO,
      montoFormateado, // 13. WRBTR (Dato de la query)
      fechaFormateada, // 14. VALUT (Fecha valor)
      textoPosicion, // 15. SGTXT (Calculado)
      '50', // 16. NEWBS_01 (Fijo)
      //'1101010051', // 17. NEWKO_01 (Fijo)
      row.CUENTA_TRANSACCION,
      montoFormateado, // 18. WRBTR_01 (Dato de la query)
      textoPosicion, // 19. SGTXT_01 (Calculado, asumimos igual a SGTXT)
    ];

    worksheet.addRow(excelRow);
  });

  return workbook.xlsx.writeBuffer();
  //return workbook.csv.writeBuffer();

  // --- 2.6 Guardar Archivo ---
  // const nombreArchivo = 'Reporte_SAP.xlsx';
  // await workbook.xlsx.writeFile(nombreArchivo);
  // console.log(`¡Reporte generado exitosamente: ${nombreArchivo}!`);
}

function formatFechaSAP(date) {
  if (!(date instanceof Date) || isNaN(date)) {
    return '';
  }
  const day = String(date.getUTCDate()).padStart(2, '0');
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const year = date.getUTCFullYear();

  // Formato: DD.MM.YYYY HH:MM:SS
  return `${day}.${month}.${year}`;
}

function formatMonto(number) {
  if (typeof number !== 'number' || isNaN(number)) {
    return '0';
  }

  return new Intl.NumberFormat('es-CL', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(number);
}

module.exports = {
  getDataDescargaExcel,
  getDataExcelPorDia,
  generarReporte,
};
