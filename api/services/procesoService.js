/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');

async function moverRegistrosCCN() {
  const connection = await getConnection();

  const insertSQL = `
    INSERT INTO CCN_TBK_HISTORICO (
      ID_CCN, DKTT_DT_REG, DKTT_DT_TYP, DKTT_DT_TC, DKTT_DT_SEQ_NUM,
      DKTT_DT_TRAN_DAT, DKTT_DT_TRAN_TIM, DKTT_DT_INST_RETAILER, DKTT_DT_ID_RETAILER,
      DKTT_DT_NAME_RETAILER, DKTT_DT_CARD, DKTT_DT_AMT_1, DKTT_DT_AMT_PROPINA, DKTT_TIPO_CUOTA,
      DKTT_DT_CANTI_CUOTAS, DKTT_DT_RESP_CDE, DKTT_DT_APPRV_CDE, DKTT_DT_TERM_NAME, DKTT_DT_ID_CAJA, 
      DKTT_DT_NUM_BOLETA, DKTT_DT_AUTH_TRACK2, DKTT_DT_FECHA_VENTA, DKTT_DT_HORA_VENTA, DKTT_DT_FECHA_PAGO, 
      DKTT_DT_COD_RECHAZO, DKTT_DT_GLOSA_RECHAZO, DKTT_DT_VAL_CUOTA, DKTT_DT_VAL_TASA, DKTT_DT_NUMERO_UNICO, 
      DKTT_DT_TIPO_MONEDA, DKTT_DT_ID_RETAILER_RE,DKTT_DT_COD_SERVICIO, DKTT_DT_VCI, DKTT_MES_GRACIA, 
      DKTT_PERIODO_GRACIA 
    )
    SELECT
      ID, DKTT_DT_REG, DKTT_DT_TYP, DKTT_DT_TC, DKTT_DT_SEQ_NUM,
      DKTT_DT_TRAN_DAT, DKTT_DT_TRAN_TIM, DKTT_DT_INST_RETAILER, DKTT_DT_ID_RETAILER,
      DKTT_DT_NAME_RETAILER, DKTT_DT_CARD, DKTT_DT_AMT_1, DKTT_DT_AMT_PROPINA, DKTT_TIPO_CUOTA,
      DKTT_DT_CANTI_CUOTAS, DKTT_DT_RESP_CDE, DKTT_DT_APPRV_CDE, DKTT_DT_TERM_NAME, DKTT_DT_ID_CAJA, 
      DKTT_DT_NUM_BOLETA, DKTT_DT_AUTH_TRACK2, DKTT_DT_FECHA_VENTA, DKTT_DT_HORA_VENTA, DKTT_DT_FECHA_PAGO, 
      DKTT_DT_COD_RECHAZO, DKTT_DT_GLOSA_RECHAZO, DKTT_DT_VAL_CUOTA, DKTT_DT_VAL_TASA, DKTT_DT_NUMERO_UNICO, 
      DKTT_DT_TIPO_MONEDA, DKTT_DT_ID_RETAILER_RE, DKTT_DT_COD_SERVICIO, DKTT_DT_VCI, DKTT_MES_GRACIA, 
      DKTT_PERIODO_GRACIA
    FROM CUADRATURA_FILE_TBK
    WHERE TIPO_TRANSACCION = 'CCN'
    AND STATUS_SAP_REGISTER = 'S'
  `;

  /*
  const deleteSQL = `
    DELETE FROM CUADRATURA_FILE_TBK
    WHERE TIPO_TRANSACCION = 'CCN'`;
    */

  try {
    await connection.execute(insertSQL);
    // const result = await connection.execute(deleteSQL);
    await connection.commit();

    return {
      //mensaje: 'Registros CCN migrados correctamente',
      mensaje: 'Registros CCN insertados correctamente (sin eliminar los temporales)',
      //registrosEliminados: result.rowsAffected,
    };
  } catch (error) {
    await connection.rollback();
    console.error('Error en moverRegistrosCCN:', error);
    throw error;
  }
}

module.exports = {
  moverRegistrosCCN,
};
