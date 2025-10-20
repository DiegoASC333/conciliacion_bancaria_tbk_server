require('dotenv').config();
const oracledb = require('oracledb');
const axios = require('axios');
const { getConnection } = require('../config/utils');
const API_KEY = 'e094aebebd85581d08c57bf20d6163a1';

// Función auxiliar que ya tenías y es reutilizable
const limpiarNombre = (nombre) => {
  if (!nombre) return 'No encontrado';
  try {
    return Buffer.from(nombre, 'latin1').toString('utf8').trim();
  } catch {
    return nombre.trim();
  }
};

const obtenerNombresSAP = async (nombresDeArchivos) => {
  if (!nombresDeArchivos || nombresDeArchivos.length === 0) {
    return { actualizados: 0, errores: 0 };
  }

  const connection = await getConnection();
  let conn;

  try {
    const sqlSelect = `
          SELECT DISTINCT
              c.ID,
              c.FILE_NAME,
              c.NOMBRE_CLIENTE,
              p.rut AS RUT
          FROM cuadratura_file_tbk c
          JOIN proceso_cupon p ON c.ID = p.id_cuadratura
          WHERE c.FILE_NAME IN (${nombresDeArchivos.map((_, i) => `:${i + 1}`).join(',')})`;

    const resultSelect = await connection.execute(sqlSelect, nombresDeArchivos, {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });
    const filasParaProcesar = resultSelect.rows || [];

    if (filasParaProcesar.length === 0) {
      return { actualizados: 0, errores: 0 };
    }

    const registrosConNombre = [];
    for (const fila of filasParaProcesar) {
      let nombre = 'No encontrado';
      if (fila.RUT) {
        try {
          const response = await axios.get(
            `https://api.utalca.cl/academia/jira/consultaClienteSap/${fila.RUT}`,
            { headers: { token: API_KEY }, timeout: 25000 }
          );
          const nombreCrudo = response.data?.nombre || 'No encontrado';
          nombre = limpiarNombre(nombreCrudo);
        } catch (error) {
          console.error(`[Enriquecimiento] Error API para RUT ${fila.RUT}: ${error.message}`);
          nombre = 'Error API';
        }
      }
      registrosConNombre.push({ id: fila.ID, nombreCliente: nombre });
    }

    let actualizados = 0;
    let errores = 0;
    const sqlUpdate = `UPDATE cuadratura_file_tbk SET NOMBRE_CLIENTE = :nombreCliente WHERE ID = :id`;

    for (const registro of registrosConNombre) {
      try {
        await connection.execute(sqlUpdate, registro); // autoCommit para asegurar que cada update se guarde
        actualizados++;
      } catch (error) {
        errores++;
      }
    }

    await connection.commit();

    return { actualizados, errores };
  } catch (err) {
    if (connection) {
      await connection.rollback();
    }
    throw err; // Re-lanzamos el error para que el llamador sepa que algo falló
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch {}
    }
  }
};

module.exports = {
  obtenerNombresSAP,
};
