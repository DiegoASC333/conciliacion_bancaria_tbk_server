function parseLCN(line) {
  return {
    LIQ_NUMC: Number(line.substring(0, 8).trim()) || null,
    LIQ_FPROC: line.substring(8, 16).trim() || null,
    LIQ_FCOM: line.substring(16, 24).trim() || null,
    LIQ_MICR: line.substring(24, 32).trim(),
    LIQ_NUMTA: line.substring(32, 51).trim(),
    LIQ_MARCA: line.substring(51, 53).trim(),
    LIQ_MONTO: Number(line.substring(53, 64).trim()) || null,
    LIQ_MONEDA: Number(line.substring(64, 65).trim()) || null,
    LIQ_TXS: line.substring(65, 67).trim(),
    LIQ_RETE: line.substring(67, 71).trim(),
    LIQ_CPRIN: Number(line.substring(71, 79).trim()) || null,
    LIQ_FPAGO: line.substring(79, 87).trim() || null,
    LIQ_ORPEDI: line.substring(87, 113).trim(),
    LIQ_CODAUT: line.substring(113, 119).trim(),
    LIQ_CUOTAS: Number(line.substring(119, 121).trim()) || null,
    LIQ_VCI: Number(line.substring(121, 125).trim()) || null,
    LIQ_CEIC: Number(line.substring(125, 136).trim()) || null,
    LIQ_CAEICA: Number(line.substring(136, 147).trim()) || null,
    LIQ_DEIC: Number(line.substring(147, 158).trim()) || null,
    LIQ_DCAEICA: Number(line.substring(158, 169).trim()) || null,
    LIQ_NTC: Number(line.substring(169, 171).trim()) || null,
    LIQ_NOMBRE_BANCO: line.substring(171, 206).trim(),
    LIQ_TIPO_CUENTA_BANCO: line.substring(206, 208).trim(),
    LIQ_NUMERO_CUENTA_BANCO: line.substring(208, 226).trim(),
    LIQ_MONEDA_CUENTA_BANCO: line.substring(226, 229).trim(),
  };
}

module.exports = parseLCN;
