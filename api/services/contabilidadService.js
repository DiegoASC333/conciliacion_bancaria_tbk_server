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
    const sql = `SELECT
        'ZA'                            doc,
        TO_DATE(l.liq_fedi, 'DD/MM/RR') AS fecha_real,
        'DEBITO'                        AS tipo,
        l.liq_fedi                      AS fecha_texto,
        SUM(l.liq_amt_1 / 100)          AS total_monto,
        l.liq_ccre                      AS codigo_comercio,
        c.nombre_comercio,
        c.cuenta_corriente,
        c.banco,
        c.cuenta_contable,
        c.descripcion,
        (SELECT tpd.cuenta_contable 
         FROM vec_cob02.tipo_pago_descripcion tpd
         WHERE tpd.clase_documento_sap = 'ZA'
         FETCH FIRST 1 ROW ONLY) AS cuenta_transaccion
    FROM
        ldn_tbk_historico         l
        LEFT JOIN vec_cob04.codigo_comerico c ON c.codigo_comerico = l.liq_ccre
    WHERE
            TO_DATE(l.liq_fedi, 'DD/MM/RR') = :fecha
        AND l.liq_ccre NOT IN ( '28208820', '48211418', '41246590', '41246593', '41246594' )
    GROUP BY
        l.liq_fedi,
        l.liq_ccre,
        c.nombre_comercio,
        c.cuenta_corriente,
        c.banco,
        c.cuenta_contable,
        c.descripcion
    UNION ALL
    SELECT
        'ZB'                           doc,
        TO_DATE(liq_fpago, 'DDMMYYYY') AS fecha_real,
        'CREDITO'                      AS tipo,
        liq_fpago                      AS fecha_texto,
        SUM(trunc(liq_monto / 100))    AS total_monto,
        NULL                           AS codigo_comercio,
        NULL                           AS nombre_comercio,
        NULL                           AS cuenta_corriente,
        NULL                           AS banco,
        '1101050021'                           AS cuenta_contable,
        NULL                           AS descripcion,
        (SELECT tpd.cuenta_contable 
         FROM vec_cob02.tipo_pago_descripcion tpd
         WHERE tpd.clase_documento_sap = 'ZB'
         FETCH FIRST 1 ROW ONLY) AS cuenta_transaccion
    FROM
        lcn_tbk_historico
    WHERE
        TO_DATE(liq_fpago, 'DDMMYYYY') = :fecha
    GROUP BY
        liq_fpago
    ORDER BY
        fecha_real,
        tipo,
        total_monto DESC `;

    const res = await connection.execute(sql, { fecha }, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return res.rows || [];
  } finally {
    try {
      await connection.close();
    } catch {}
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

  worksheet.getColumn('B').numFmt = 'dd/mm/yyyy'; // BLDAT
  worksheet.getColumn('E').numFmt = 'dd/mm/yyyy'; // BUDAT
  worksheet.getColumn('N').numFmt = 'dd/mm/yyyy'; // VALUT

  worksheet.getColumn('M').numFmt = '#,##0'; // WRBTR
  worksheet.getColumn('R').numFmt = '#,##0'; // WRBTR_01

  const mesContableNum = fechaConsulta.getMonth() + 1;
  const mesContableStr = mesContableNum.toString().padStart(2, '0');
  const anioConsulta = fechaConsulta.getFullYear();
  const textoPosicion = `ABONO EN CTA CTE. RECAUDACION TRANSBANK ${mesContableStr}${anioConsulta}`;

  dataRows.forEach((row, index) => {
    //const incremental = index + 1;

    const excelRow = [
      row.DOC, // 1. DOC
      row.FECHA_REAL, // 2. BLDAT (Fecha doc)
      'CB', // 3. BLART (Fijo)
      'UT01', // 4. BURSK (Fijo de headerRow2)
      row.FECHA_REAL, // 5. BUDAT (Fecha contab)
      mesContableNum, // 6. MONAT (Calculado)
      'CLP', // 7. WAERS (Fijo)
      'TRANSBANK', // 8. XBLNR (Fijo)
      `CARTOLA 01`, // 9. BKTXT (Calculado)
      '*/', // 10. DOCID (Fijo)
      '40', // 11. NEWBS (Fijo)
      //row.CUENTA_CORRIENTE, // 12. NEWKO (Dato de la query, será null para 'ZB')
      row.CUENTA_CONTABLE,
      row.TOTAL_MONTO, // 13. WRBTR (Dato de la query)
      row.FECHA_REAL, // 14. VALUT (Fecha valor)
      textoPosicion, // 15. SGTXT (Calculado)
      '50', // 16. NEWBS_01 (Fijo)
      //'1101010051', // 17. NEWKO_01 (Fijo)
      row.CUENTA_TRANSACCION,
      row.TOTAL_MONTO, // 18. WRBTR_01 (Dato de la query)
      textoPosicion, // 19. SGTXT_01 (Calculado, asumimos igual a SGTXT)
    ];

    worksheet.addRow(excelRow);
  });

  return workbook.xlsx.writeBuffer();

  // --- 2.6 Guardar Archivo ---
  // const nombreArchivo = 'Reporte_SAP.xlsx';
  // await workbook.xlsx.writeFile(nombreArchivo);
  // console.log(`¡Reporte generado exitosamente: ${nombreArchivo}!`);
}

module.exports = {
  getDataDescargaExcel,
  getDataExcelPorDia,
  generarReporte,
};
