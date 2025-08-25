/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');

async function moverRegistrosCDN() {
  const connection = await getConnection();

  const insertSQL = `
    INSERT INTO CDN_TBK_HISTORICO (
      ID_CDN, DSK_DT_REG, DSK_TYP, DSK_TC, DSK_TRAN_DAT, DSK_TRAN_TIM, 
      DSK_ID_RETAILER, DSK_NAME_RETAILER, DSK_CARD, DSK_AMT_1, DSK_AMT_2,
      DSK_AMT_PROPINA, DSK_RESP_CDE, DSK_APPVR_CDE, DSK_TERMN_NAME, 
      DSK_ID_CAJA, DSK_NUM_BOLETA, DSK_FECHA_PAGO, DSK_IDENT, DSK_ID_RETAILER_2, 
      DSK_ID_COD_SERVI, DSK_ID_NRO_UNICO, DSK_PREPAGO
    )
    SELECT
      ID, DKTT_DT_REG, DKTT_DT_TYP, DKTT_DT_TC,DKTT_DT_TRAN_DAT, DKTT_DT_TRAN_TIM,
      DKTT_DT_ID_RETAILER, DKTT_DT_NAME_RETAILER, DKTT_DT_CARD, DKTT_DT_AMT_1,
      DSK2_AMT_2, DKTT_DT_AMT_PROPINA, DKTT_DT_RESP_CDE, DKTT_DT_APPRV_CDE,
      DKTT_DT_TERM_NAME, DKTT_DT_ID_CAJA, DKTT_DT_NUM_BOLETA, DKTT_DT_FECHA_VENTA, 
      DSK_IDENT, DKTT_DT_ID_RETAILER_RE, DSK_ID_COD_SERVI, DSK_ID_NRO_UNICO, DSK_PREPAGO
    FROM CUADRATURA_FILE_TBK
    WHERE TIPO_TRANSACCION = 'CDN'
    AND STATUS_SAP_REGISTER = 'ENCONTRADO' `;

  try {
    const result = await connection.execute(insertSQL);
    await connection.commit();
    return {
      mensaje: 'Registros CDN insertados correctamente',
      registrosInsertados: result.rowsAffected,
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  }
}

module.exports = {
  moverRegistrosCDN,
};
