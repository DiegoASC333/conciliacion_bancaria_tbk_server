function parseCCN(line) {
  return {
    DKTT_DT_REG: line.substring(0, 2).trim(), // 2
    DKTT_DT_TYP: Number(line.substring(2, 6).trim()) || null, // 4
    DKTT_DT_TC: Number(line.substring(6, 8).trim()) || null, // 2
    DKTT_DT_SEQ_NUM: line.substring(8, 20).trim(), // 12
    DKTT_DT_TRAN_DAT: line.substring(20, 26).trim() || null, // 6
    DKTT_DT_TRAN_TIM: line.substring(26, 32).trim() || null, // 6
    DKTT_DT_INST_RETAILER: line.substring(32, 36).trim(), // 4
    DKTT_DT_ID_RETAILER: Number(line.substring(36, 44).trim()) || null, // 8
    DKTT_DT_NAME_RETAILER: line.substring(44, 64).trim(), // 20
    DKTT_DT_CARD: line.substring(64, 83).trim(), // 19
    DKTT_DT_AMT_1: Number(line.substring(83, 96).trim()) || null, // 13
    DKTT_DT_AMT_PROPINA: Number(line.substring(96, 105).trim()) || null, // 9
    DKTT_TIPO_CUOTA: Number(line.substring(105, 106).trim()) || null, // 1
    DKTT_DT_CANTI_CUOTAS: Number(line.substring(106, 108).trim()) || null, // 2
    DKTT_DT_RESP_CDE: Number(line.substring(108, 111).trim()) || null, // 3
    DKTT_DT_APPRV_CDE: line.substring(111, 119).trim(), // 8
    DKTT_DT_TERM_NAME: line.substring(119, 135).trim(), // 16
    DKTT_DT_ID_CAJA: line.substring(135, 151).trim(), // 16
    DKTT_DT_NUM_BOLETA: line.substring(151, 161).trim(), // 10
    DKTT_DT_AUTH_TRACK2: line.substring(161, 162).trim(), // 1
    DKTT_DT_FECHA_VENTA: line.substring(162, 168).trim() || null, // 6
    DKTT_DT_HORA_VENTA: line.substring(168, 174).trim() || null, // 6
    DKTT_DT_FECHA_PAGO: line.substring(174, 180).trim() || null, // 6
    DKTT_DT_COD_RECHAZO: line.substring(180, 183).trim(), // 3
    DKTT_DT_GLOSA_RECHAZO: line.substring(183, 203).trim(), // 20
    DKTT_DT_VAL_CUOTA: Number(line.substring(203, 212).trim()) || null, // 9
    DKTT_DT_VAL_TASA: Number(line.substring(212, 216).trim()) || null, // 4
    DKTT_DT_NUMERO_UNICO: line.substring(216, 242).trim(), // 26
    DKTT_DT_TIPO_MONEDA: Number(line.substring(242, 243).trim()) || null, // 1
    DKTT_DT_ID_RETAILER_RE: Number(line.substring(243, 251).trim()) || null, // 8
    DKTT_DT_COD_SERVICIO: line.substring(251, 271).trim(), // 20
    DKTT_DT_VCI: line.substring(271, 275).trim(), // 4
    DKTT_MES_GRACIA: line.substring(275, 276).trim(), // 1
    DKTT_PERIODO_GRACIA: line.substring(276, 278).trim(), // 2
  };
}

module.exports = parseCCN;
