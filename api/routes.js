const express = require('express');
const router = express.Router();

//const sshController = require('./controllers/sshController');
const { ejecutarScript } = require('./controllers/sshController');
//const sftpController = require('./controllers/sftpController');
const { listarArchivosNuevos } = require('./controllers/sftpController');
const { testProcesoCadena } = require('./controllers/testController'); //probar flujo
const { guardarRegistrosCCN } = require('./controllers/procesoController'); // mover registros CCN
const { guardarRegistrosCDN } = require('./controllers/procesoControllerCDN');
const { procesarArchivoPorNombre } = require('./controllers/procesarArchivoManualController.js');
const { procesarArchivosPorFecha } = require('./controllers/procesarxFechasController.js');
const { procesarArchivosRemotos } = require('./controllers/procesarDatGzController.js');
const { procesarArchivosRemotosAutomatico } = require('./controllers/procesoActualController.js');
const { getStatusCuadratura } = require('./controllers/statusCuadraturaController.js');
const { listarporTipo } = require('./controllers/statusCuadraturaController.js');
//Rutas
router.post('/ejecutar-script', ejecutarScript); //ejecucion manual de script remoto
router.get('/listar-archivos-nuevos', listarArchivosNuevos); // prueba para listar archivos en servidor
router.post('/test-proceso-cadena', testProcesoCadena); //prueba de flujo
router.post('/guardar-registros-ccn', guardarRegistrosCCN);
router.post('/guardar-registros-cdn', guardarRegistrosCDN);
router.post('/procesar-archivo', procesarArchivoPorNombre); //procesar archivo manual por nombre
router.post('/procesar-archivos-por-fecha', procesarArchivosPorFecha); // procesar archivos por rango de fechas, solo DAT
router.post('/procesar-dat-gz', procesarArchivosRemotos); // procesar archivos remotos .DAT y .DAT.GZ con rango de fechas
router.post('/procesar-archivos-remotos-automatico', procesarArchivosRemotosAutomatico); //procesar archivos remotos automáticos
router.get('/status-cuadratura', getStatusCuadratura); // obtener estado de cuadratura diario
router.get('/status-cuadratura/:tipo', listarporTipo); //listar por tipo los registros

module.exports = router;
