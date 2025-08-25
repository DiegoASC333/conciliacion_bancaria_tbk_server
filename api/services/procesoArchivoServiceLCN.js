/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');

async function moverRegistrosLCN() {
  const connection = await getConnection();

  const insertSQL = `
    INSERT INTO LCN_TBK_HISTORICO(
    ID_LCN, LIQ_NUMC, LIQ_FPROC, LIQ_FCOM, LIQ_MICR, LIQ_NUMTA, LIQ_MARCA,
    LIQ_MONTO, LIQ_MONEDA, LIQ_TXS, LIQ_RETE, LIQ_CPRIN, LIQ_FPAGO, LIQ_ORPEDI,
    LIQ_CODAUT, LIQ_CUOTAS, LIQ_VCI, LIQ_CEIC, LIQ_CAEICA, LIQ_DEIC, 
    LIQ_DCAEICA, LIQ_NTC, LIQ_NOMBRE_BANCO, LIQ_TIPO_CUENTA_BANCO, 
    LIQ_NUMERO_CUENTA_BANCO, LIQ_MONEDA_CUENTA_BANCO
    ) 
    SELECT LIQ_NUMC, LIQ_FPROC, LIQ_FCOM, LIQ_MICR, LIQ_NUMTA, LIQ_MARCA,
    LIQ_MONTO, LIQ_MONEDA, LIQ_TXS, LIQ_RETE, LIQ_CPRIN, LIQ_FPAGO, LIQ_ORPEDI,
    LIQ_CODAUT, LIQ_CUOTAS, LIQ_VCI, LIQ_CEIC, LIQ_CAEICA, LIQ_DEIC, LIQ_DCAEICA
    LIQ_NTC, LIQ_NOMBRE_BANCO, LIQ_TIPO_CUENTA_BANCO, LIQ_NUMERO_CUENTA_BANCO,
    LIQ_MONEDA_CUENTA_BANCO
    FROM LIQUIDACION_FILE_TBK LFT, CUADRATURA_FILE_TBK CFT
    WHERE LFT.TIPO_TRANSACCION = 'LCN'
    AND LFT.LIQ_ORPEDI = CFT.DKTT_TRAN_DAT `;

  try {
    const result = await connection.execute(insertSQL);
    await connection.commit();
    return {
      mensaje: 'Registros LCN insertados correctamente',
      registrosInsertados: result.rowsAffected,
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  }
}

module.exports = {
  moverRegistrosLCN,
};
