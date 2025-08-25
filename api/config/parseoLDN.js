function parseLDN(line) {
  return {
    LIQ_NUMC: Number(line.substring(0, 8).trim()) || null, // 8
    LIQ_FPROC: line.substring(8, 14).trim() || null, // 6
    LIQ_FCOM: line.substring(14, 20).trim() || null, // 6
    LIQ_APPR: line.substring(20, 26).trim(), // 6
    LIQ_NUMTA: line.substring(26, 45).trim(), // 19
    LIQ_MONTO: Number(line.substring(45, 58).trim()) || null, // 13
    LIQ_TXS: line.substring(58, 60).trim(), // 2
    LIQ_CPRI: line.substring(60, 68).trim(), // 8
    LIQ_MARC: line.substring(68, 70).trim(), // 2
    LIQ_FEDI: line.substring(70, 78).trim(), // 8
    LIQ_NRO_UNICO: line.substring(78, 104).trim(), // 26
    LIQ_CEIC: Number(line.substring(104, 117).trim()) || null, // 13
    LIQ_CAD_CADIVA: line.substring(117, 130).trim(), // 13
    LIQ_DEIC: Number(line.substring(130, 143).trim()) || null, // 13
    LIQ_DCOAD_IVCOM: line.substring(143, 156).trim(), // 13
    LIQ_PREPAGO: line.substring(156, 157).trim(), // 1
    LIQ_NOMBRE_BANCO: line.substring(157, 192).trim(), // 35
    LIQ_TIPO_CUENTA_BANCO: line.substring(192, 194).trim(), // 2
    LIQ_NUMERO_CUENTA_BANCO: line.substring(194, 212).trim(), // 18
    LIQ_MONEDA_CUENTA_BANCO: line.substring(212, 215).trim(), // 3
  };
}

module.exports = parseLDN;
