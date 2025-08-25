/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');

async function moverRegistrosLDN() {
  const connection = await getConnection();

  const insertSQL = `
    INSERT INTO LDN_TBK_HISTORICO(
    ID_LDN, LIQ_CCRE, LIQ_FPRO, LIQ_FCOM, LIQ_APPR, LIQ_PAN, LIQ_AMT_1, LIQ_TTRA, 
    LIQ_CPRI, LIQ_MARC, LIQ_FEDI, LIQ_NRO_UNICO, LIQ_COM_COMIV, LIQ_CAD_CADIVA, 
    LIQ_DECOM_IVCOM, LIQ_DCOAD_IVCOM, LIQ_PREPAGO, LIQ_NOMBRE_BANCO, LIQ_TIPO_CUENTA_BANCO, 
    LIQ_NUMERO_CUENTA_BANCO, LIQ_MONEDA_CUENTA_BANCO
    )
    SELECT ID, LIQ_NUMC, LIQ_FPROC,LIQ_FCOM, LIQ_APPR, LIQ_NUMTA, LIQ_MONTO, LIQ_TXS, LIQ_CPRI,LIQ_MARC
    LIQ_FEDI, LIQ_NRO_UNICO, LIQ_CEIC, LIQ_CAD_CADIVA, LIQ_DEIC, LIQ_DCOAD_IVCOM, LIQ_PREPAGO,
    LIQ_NOMBRE_BANCO, LIQ_TIPO_CUENTA_BANCO, LIQ_NUMERO_CUENTA_BANCO, LIQ_MONEDA_CUENTA_BANCO 
    FROM LIQUIDACION_FILE_TBK LFT, CUADRATURA_FILE_TBK CFT
    WHERE LFT.TIPO_TRANSACCION = 'LDN' 
    AND LFT.LIQ_ORPEDI = CFT.DKTT_TRAN_DAT`;

  try {
    const result = await connection.execute(insertSQL);
    await connection.commit();
    return {
      mensaje: 'Registros LDN insertados correctamente',
      registrosInsertados: result.rowsAffected,
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  }
}

module.exports = {
  moverRegistrosLDN,
};
