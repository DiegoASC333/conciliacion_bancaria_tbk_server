/* eslint-disable no-undef */
const { getConnection } = require('../config/utils');
const oracledb = require('oracledb');

/**
 * Lee los cupones de la tabla CUADRATURA_FILE_TBK e inserta solo los cupones existentes
 * y válidos, priorizando débito/crédito y usando la columna de respaldo si es necesario.
 *
 * @param {string} tablaDestino - Nombre de la tabla donde se insertarán los cupones.
 * @returns {Promise<object>} Objeto con el resultado de la operación.
 */
async function procesarCupones(tablaDestino = 'PROCESO_CUPON') {
  let connection;

  try {
    connection = await getConnection();

    // Consulta para obtener las columnas necesarias, incluyendo la nueva columna de respaldo.
    const sqlSelect = `
      SELECT
        ID AS ID_CUADRATURA,
        DKTT_DT_TRAN_DAT AS FECHA, 
        TIPO_TRANSACCION ,
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

      const esValido = (valor) => valor && !isNaN(valor) && Number(valor) > 0;

      // 1. Prioridad alta: cupón de crédito (CCN)
      if (fila.TIPO_TRANSACCION === 'CCN' && esValido(fila.CUPON_CREDITO)) {
        cuponValor = fila.CUPON_CREDITO;
        tipoCupon = 'CCN';
        // 2. Prioridad media: cupón de débito (CDN)
      } else if (fila.TIPO_TRANSACCION === 'CDN' && esValido(fila.CUPON_DEBITO)) {
        cuponValor = fila.CUPON_DEBITO;
        tipoCupon = 'CDN';
        // 3. Prioridad baja: cupón de respaldo si no hay débito ni crédito
      } else if (fila.CUPON_CAJA) {
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

    // Armar y ejecutar la consulta de inserción.
    /*
    const sqlInsert = `
      INSERT INTO ${tablaDestino} (ID,ID_CUADRATURA, CUPON, FECHA, TIPO_TRANSACCION)
      VALUES (SEQ_CUPONES_ID.NEXTVAL, :ID, :CUPON, :FECHA, :TIPO_TRANSACCION)
    `;
    */
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
      ON (D.CUPON = S.CUPON)
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
