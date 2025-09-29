// index.js
const app = require('./app');

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
});
// app.listen(PORT, '0.0.0.0', () => {
//   console.log(`Servidor corriendo en http://0.0.0.0:${PORT}`);
// });
