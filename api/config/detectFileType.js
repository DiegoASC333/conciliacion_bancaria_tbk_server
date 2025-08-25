function detectarTipoArchivo(lineaHeader, nombreArchivo) {
  if (lineaHeader.startsWith('HR') && nombreArchivo.includes('CCN')) return 'CCN';
  if (lineaHeader.startsWith('HR') && nombreArchivo.includes('CDN')) return 'CDN';
  if (lineaHeader.includes('HEADER') && nombreArchivo.includes('LCN')) return 'LCN';
  if (lineaHeader.includes('HEADER') && nombreArchivo.includes('LDN')) return 'LDN';

  throw new Error(`No se puede determinar el tipo de archivo: ${nombreArchivo}`);
}

module.exports = { detectarTipoArchivo };
