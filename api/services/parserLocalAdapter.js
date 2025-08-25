const SftpClient = require('ssh2-sftp-client');
const fs = require('fs/promises');
const path = require('path');

// Importa todos tus parseadores
const parseCCN = require('../config/parseoCCN');
const parseCDN = require('../config/parseoCDN');
const parseLCN = require('../config/parseoLCN');
const parseLDN = require('../config/parseoLDN');

// Mapeo de parseadores
const PARSERS = {
  CCN: parseCCN,
  CDN: parseCDN,
  LCN: parseLCN,
  LDN: parseLDN,
};

// Función simple para obtener el tipo a partir del nombre lógico
function getTipoDesdeNombre(nombreArchivoLogico) {
  return (nombreArchivoLogico.split('_')[0] || '').toUpperCase();
}

/**
 * Parsea el contenido de un archivo (ya sea un buffer o un string) y retorna los registros.
 * @param {Buffer|string} contenido - El contenido del archivo a parsear.
 * @param {string} nombreArchivoLogico - El nombre lógico del archivo (ej. CCN_20240101.dat).
 * @returns {{tipo: string, registros: Array<object>}} Un objeto con el tipo de archivo y los registros parseados.
 */
function parsearContenido(contenido, nombreArchivoLogico) {
  const lineas = contenido
    .toString('utf-8')
    .split(/\r?\n/)
    .filter((line) => line.trim() !== '');

  if (lineas.length < 3) {
    throw new Error('El archivo no contiene suficiente contenido útil.');
  }

  const tipo = getTipoDesdeNombre(nombreArchivoLogico);
  const parser = PARSERS[tipo];

  if (!parser) {
    throw new Error(`Parser no definido para tipo de archivo: ${tipo}`);
  }

  const lineasDetalle = lineas.slice(1, -1);
  const registros = lineasDetalle
    .map(parser)
    .filter((r) => !!r)
    .map((r) => {
      const limpio = {};
      for (const key in r) {
        const val = r[key];
        limpio[key] = typeof val === 'string' && val.trim() === '' ? null : val;
      }

      return {
        TIPO_TRANSACCION: tipo,
        FILE_NAME: nombreArchivoLogico,
        STATUS_SAP_REGISTER: 'PENDIENTE',
        STATUS_SAP_DATE: new Date(),
        DATE_LOAD_BBDD: new Date(),
        ...limpio,
      };
    });

  return { tipo, registros };
}

/**
 * Lee un archivo local y utiliza la función de parseo unificada.
 * Este es el punto de entrada para el controlador.
 * @param {{rutaLocal: string, nombreArchivoLogico: string}} opciones - Objeto con la ruta local y nombre lógico.
 * @returns {{tipo: string, registros: Array<object>}}
 */
async function leerYParsearArchivoLocal(opciones) {
  const { rutaLocal, nombreArchivoLogico } = opciones;
  const contenido = await fs.readFile(rutaLocal);
  return parsearContenido(contenido, nombreArchivoLogico);
}

module.exports = {
  leerYParsearArchivoLocal,
};
