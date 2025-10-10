function parseCDN(line) {
  return {
    DKTT_DT_REG: line.substring(0, 2).trim(), // 2
    DKTT_DT_TYP: Number(line.substring(2, 6).trim()) || null, // 4
    DKTT_DT_TC: Number(line.substring(6, 8).trim()) || null, // 2
    DKTT_DT_TRAN_DAT: line.substring(8, 14).trim() || null, // 6
    DKTT_DT_TRAN_TIM: line.substring(14, 20).trim() || null, // 6
    DKTT_DT_ID_RETAILER: Number(line.substring(20, 32).trim()) || null, // 12
    DKTT_DT_NAME_RETAILER: line.substring(32, 52).trim(), // 20
    DKTT_DT_CARD: line.substring(52, 71).trim(), // 19
    DKTT_DT_AMT_1: Number(line.substring(71, 84).trim()) || null, // 13
    DSK2_AMT_2: line.substring(84, 97).trim(), // 13
    DKTT_DT_AMT_PROPINA: Number(line.substring(97, 106).trim()) || null, // 9
    DKTT_DT_RESP_CDE: Number(line.substring(106, 109).trim()) || null, // 3
    DKTT_DT_APPRV_CDE: line.substring(109, 117).trim(), // 8
    DKTT_DT_TERM_NAME: line.substring(117, 133).trim(), // 16
    DKTT_DT_ID_CAJA: line.substring(133, 149).trim(), // 16
    DKTT_DT_NUM_BOLETA: line.substring(149, 159).trim(), // 10
    DKTT_DT_FECHA_PAGO: line.substring(159, 165).trim() || null, // 6 cambiar a fecha_pago
    DSK_IDENT: line.substring(165, 167).trim(), // 2
    DKTT_DT_ID_RETAILER_RE: Number(line.substring(167, 175).trim()) || null, // 8
    DSK_ID_COD_SERVI: line.substring(175, 195).trim(), // 20
    DSK_ID_NRO_UNICO: line.substring(195, 221).trim(), // 26
    DSK_PREPAGO: line.substring(221, 222).trim(), // 1
  };
}

module.exports = parseCDN;
