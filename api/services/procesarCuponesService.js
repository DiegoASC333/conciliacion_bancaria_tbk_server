/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

async function procesarCupones(tablaDestino = 'PROCESO_CUPON') {
  let connection;

  const esCuponNumericoValido = (cuponStr) => {
    // 1. Verificar que sea un string no nulo y no vacío.
    if (!cuponStr || typeof cuponStr !== 'string' || cuponStr.trim() === '') {
      return false;
    }
    // 2. Usar una expresión regular para asegurar que solo contenga dígitos.
    const soloNumerosRegex = /^[0-9]+$/;
    if (!soloNumerosRegex.test(cuponStr)) {
      return false;
    }
    // 3. Convertir a número y verificar que no sea 0 (esto elimina "0", "00", "0000", etc.).
    if (Number(cuponStr) <= 0) {
      return false;
    }
    return true;
  };

  try {
    connection = await getConnection();

    // Consulta para obtener las columnas necesarias, incluyendo la nueva columna de respaldo.
    const sqlSelect = `
      SELECT
        ID AS ID_CUADRATURA,
        DKTT_DT_TRAN_DAT AS FECHA, 
        TIPO_TRANSACCION,
        DSK_ID_NRO_UNICO AS CUPON_DEBITO,
        DKTT_DT_NUMERO_UNICO AS CUPON_CREDITO,
        DKTT_DT_APPRV_CDE AS CUPON_CAJA
      FROM
        CUADRATURA_FILE_TBK
    `;

    const result = await connection.execute(sqlSelect, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });

    const cupones = result.rows;

    if (!cupones || cupones.length === 0) {
      console.log('No se encontraron cupones para procesar.');
      return { exito: true, cantidad: 0, mensaje: 'No se encontraron cupones.' };
    }

    // Preparar los binds, filtrando los cupones según la nueva lógica de prioridad.
    const bindsParaInsertar = [];
    cupones.forEach((fila) => {
      let id = fila.ID_CUADRATURA;
      let fecha = fila.FECHA;
      let cuponValor = null;
      let tipoCupon = null;

      // LÓGICA DE PRIORIDAD ACTUALIZADA CON LAS NUEVAS VALIDACIONES
      // 1. Prioridad alta: cupón de crédito (CCN), debe ser numérico y válido.
      if (fila.TIPO_TRANSACCION === 'CCN' && esCuponNumericoValido(fila.CUPON_CREDITO)) {
        cuponValor = fila.CUPON_CREDITO;
        tipoCupon = 'CCN';
        // 2. Prioridad media: cupón de débito (CDN), debe ser numérico y válido.
      } else if (fila.TIPO_TRANSACCION === 'CDN' && esCuponNumericoValido(fila.CUPON_DEBITO)) {
        cuponValor = fila.CUPON_DEBITO;
        tipoCupon = 'CDN';
        // 3. Prioridad baja: cupón de respaldo (CAJA), solo necesita existir (puede ser alfanumérico).
      } else if (fila.CUPON_CAJA && fila.CUPON_CAJA.trim() !== '') {
        cuponValor = fila.CUPON_CAJA;
        tipoCupon = 'CAJA';
      }

      // Validar que el cupón sea un número y mayor que cero antes de insertarlo.
      if (cuponValor) {
        bindsParaInsertar.push({
          ID: id,
          FECHA: fecha,
          CUPON: cuponValor,
          TIPO_TRANSACCION: tipoCupon,
        });
      }
    });

    if (bindsParaInsertar.length === 0) {
      console.log('No se encontraron cupones válidos para insertar.');
      return { exito: true, cantidad: 0, mensaje: 'No se encontraron cupones válidos.' };
    }
    //se agrega nueva validación para fecha distintas mismo cupón
    const sqlMerge = `
      MERGE INTO ${tablaDestino} D
      USING (
        SELECT
          :ID AS ID_CUDARATURA,
          :FECHA AS FECHA,
          :CUPON AS CUPON,
          :TIPO_TRANSACCION AS TIPO_TRANSACCION
        FROM DUAL
      ) S
      ON (D.CUPON = S.CUPON AND D.FECHA = S.FECHA) -- SE VALIDA POR CUPÓN Y FECHA
      WHEN NOT MATCHED THEN
      INSERT (ID, ID_CUADRATURA, CUPON, FECHA, TIPO_TRANSACCION)
      VALUES (SEQ_CUPONES_ID.NEXTVAL, S.ID_CUDARATURA, S.CUPON, S.FECHA, S.TIPO_TRANSACCION)
    `;

    const options = { autoCommit: false, batchErrors: true };
    const insertResult = await connection.executeMany(sqlMerge, bindsParaInsertar, options);

    await connection.commit();
    console.log(`Se insertaron ${insertResult.rowsAffected} registros de cupones.`);

    return {
      exito: true,
      cantidad: insertResult.rowsAffected,
    };
  } catch (error) {
    console.error('Error al procesar los cupones:', error);
    if (connection) {
      await connection.rollback();
    }
    throw error;
  } finally {
    if (connection) {
      await connection.close();
      console.log('Conexión cerrada.');
    }
  }
}

module.exports = {
  procesarCupones,
};
