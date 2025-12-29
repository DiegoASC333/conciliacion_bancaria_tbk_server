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
const {
  getStatusCuadratura,
  listarporTipo,
  exportarReporteCompletoExcel,
} = require('./controllers/statusCuadraturaController.js');
const { reprocesoCuponController } = require('./controllers/reprocesoCuponController.js');
const {
  postEnviarTesoreria,
  validarFechasAnteriores,
} = require('./controllers/auditoriaDafeController.js');
const {
  getLiquidacionController,
  getLiquidacionxls,
  validarLiquidacionController,
} = require('./controllers/liquidacionController.js');
const {
  getCartolaTesoreriaController,
  getDataHistorialRut,
  getCartolaxls,
} = require('./controllers/cartolaTesoreriaController.js');
const { login, loginBack } = require('./controllers/authController.js');
const {
  procesarArchivosRemotosPorNombre,
} = require('./controllers/procesarArchivoPorNombreController.js');
const { getVentasController } = require('./controllers/reporteVentaController.js');

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
router.get('/status-cuadratura/:fecha/:perfil', getStatusCuadratura); // obtener estado de cuadratura diario
router.get('/status-cuadratura/:fecha/:tipo/:tipoTransaccion/:perfil', listarporTipo); //listar por tipo los registros
router.post('/reproceso-cupon', reprocesoCuponController); // reprocesar cupon
router.post('/auditoria-dafe', postEnviarTesoreria); // auditoría y envío a tesorería
router.post('/liquidacion', getLiquidacionController); // obtener liquidacion por tipo
router.post('/cartola-tbk', getCartolaTesoreriaController); //obtener cartola
router.post('/historial-rut', getDataHistorialRut); // obtener data de cartola por rut
router.post('/liquidacion-excel', getLiquidacionxls); //obtener excel de liquidaciones
router.post('/cartola-excel', getCartolaxls); //obtener excel de cartola
router.post('/exportar-excel-completo', exportarReporteCompletoExcel); // excel para obtener cuadratura
router.get('/cuadratura/validacion/fechas-anteriores/:fecha', validarFechasAnteriores); // obtener fechas anteriores a la indicada
router.post('/validar', validarLiquidacionController); // validación liquidacion
router.post('/login', login); //login
router.post('/login-back', loginBack); //loginBack;
router.post('/procesar-archivo-por-nombre', procesarArchivosRemotosPorNombre); //procesar archivo manual por nombre
router.post('/reporte-ventas', getVentasController); // reporte de ventas por transacción tipo

module.exports = router;
