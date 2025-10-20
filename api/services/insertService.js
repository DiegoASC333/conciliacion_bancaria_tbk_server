/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

function getTablaDestino(tipo) {
  if (['CCN', 'CDN'].includes(tipo)) return 'CUADRATURA_FILE_TBK';
  if (['LCN', 'LDN'].includes(tipo)) return 'LIQUIDACION_FILE_TBK';
  throw new Error(`Tipo de archivo no reconocido: ${tipo}`);
}
function getSecuenciaPorTipo(tipo) {
  if (['CCN', 'CDN'].includes(tipo)) return 'SEQ_CUADRATURA_ID';
  if (['LCN', 'LDN'].includes(tipo)) return 'SEQ_LIQUIDACION_ID';
  throw new Error(`Tipo de archivo no reconocido para secuencia: ${tipo}`);
}

async function insertarRegistros({ tipo, registros, nombreArchivo }) {
  if (!registros || registros.length === 0) {
    return 0;
  }
  /*validar archivo existente*/
  const yaExiste = await verificarArchivoProcesado(nombreArchivo);
  if (yaExiste) {
    // console.log(`Archivo ${nombreArchivo} ya fue procesado, omitiendo inserción.`);
    return {
      estado: 'omitido',
      mensaje: `Archivo ${nombreArchivo} ya fue procesado.`,
    };
  }
  /*Validar archivo existente*/

  const tabla = getTablaDestino(tipo);
  const connection = await getConnection();

  try {
    //  Paso 1: preparar columnas
    let columnas = Object.keys(registros[0]);
    const usaSecuenciaID = columnas.includes('ID');
    if (usaSecuenciaID) {
      columnas = columnas.filter((col) => col !== 'ID'); // quitamos ID del bind
    }

    // Paso 2: armar SQL con secuencia
    const camposInsert = ['ID', ...columnas].join(', ');
    const secuencia = getSecuenciaPorTipo(tipo);
    const valoresInsert = [`${secuencia}.NEXTVAL`, ...columnas.map((col) => `:${col}`)].join(', ');
    const sql = `INSERT INTO ${tabla} (${camposInsert}) VALUES (${valoresInsert})`;

    // Paso 3: preparar binds sin campo ID
    const binds = registros.map((reg) => {
      const bind = {};
      columnas.forEach((col) => {
        const valor = reg[col];
        bind[col] = typeof valor === 'string' && valor.trim() === '' ? null : valor;
      });
      return bind;
    });

    const options = { autoCommit: false, batchErrors: true, dmlRowCounts: true };

    //console.log('Ejecutando insert con', binds.length, 'registros');

    let result;
    try {
      console.log('--- Registros a insertar ---');
      binds.forEach((registro, index) => {
        //console.log(`Registro ${index + 1}:`);
        Object.entries(registro).forEach(([campo, valor]) => {
          //console.log(
          //  `  ${campo}: "${valor}" (length: ${typeof valor === 'string' ? valor.length : 'n/a'})`
          //);
        });
        // console.log('------------------------------');
      });

      result = await connection.executeMany(sql, binds, options);

      if (result.batchErrors?.length > 0) {
        result.batchErrors.forEach((err, index) => {
          // console.error(`Error en registro ${index}: ${err.message}`);
          // console.log('Registro problemático:', binds[index]);
        });
      }

      // console.log('rowsAffected:', result.rowsAffected);
    } catch (e) {
      console.error('Fallo en executeMany. Mostrando primer registro problemático...');
      console.error(JSON.stringify(binds[0], null, 2));
      throw e;
    }

    /*Ejecutar funcion para validar cupones*/
    //await connection.execute(`BEGIN VEC_COB04.VALIDA_CUPON; END;`);
    /*Fin función valida cupones*/

    await registrarLogArchivo(connection, {
      nombreArchivo,
      tipoArchivo: tipo,
      estado: 'PROCESADO',
      registrosInsertados: result.rowsAffected,
    });

    await connection.commit();

    return {
      exito: true,
      cantidad: result.rowsAffected,
    };
  } catch (error) {
    // console.error('Error al insertar registros:', error);
    await registrarLogArchivo(connection, {
      nombreArchivo,
      tipoArchivo: tipo,
      estado: 'ERROR',
      mensajeError: error.message,
      registrosInsertados: 0,
    });
    throw error;
  } finally {
    await connection.close();
    // console.log('[InsertService] Conexión cerrada');
  }
}

async function registrarLogArchivo(
  connection,
  { nombreArchivo, tipoArchivo, estado, mensajeError = null, registrosInsertados = 0 }
) {
  const sql = `
    INSERT INTO LOG_ARCHIVOS_PROCESADOS (
      ID, NOMBRE_ARCHIVO, TIPO_ARCHIVO, ESTADO, MENSAJE_ERROR, REGISTROS_INSERTADOS
    ) VALUES (
      SEQ_LOG_ARCHIVOS_ID.NEXTVAL, :nombreArchivo, :tipoArchivo, :estado, :mensajeError, :registrosInsertados
    )
  `;

  try {
    const result = await connection.execute(sql, {
      nombreArchivo,
      tipoArchivo,
      estado,
      mensajeError,
      registrosInsertados,
    });
    /*
    console.log('Log registrado correctamente:', {
      nombreArchivo,
      tipoArchivo,
      estado,
      registrosInsertados,
    });
*/
    return result;
  } catch (error) {
    console.error('Error al insertar en LOG_ARCHIVOS_PROCESADOS:', error);
    throw error;
  }
}
/*Validar archuivo existente*/
async function verificarArchivoProcesado(nombreArchivo) {
  const connection = await getConnection();
  const result = await connection.execute(
    `SELECT COUNT(*) AS cantidad FROM LOG_ARCHIVOS_PROCESADOS WHERE NOMBRE_ARCHIVO = :nombre`,
    [nombreArchivo],
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  return result.rows[0].CANTIDAD > 0;
}

module.exports = {
  insertarRegistros,
  verificarArchivoProcesado,
};
