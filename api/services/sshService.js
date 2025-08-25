const { Client } = require('ssh2');

function ejecutarScriptRemoto({ host, port, username, password, rutaScript }) {
  return new Promise((resolve, reject) => {
    const conn = new Client();
    const scriptPath = process.env.REMOTE_SCRIPT;

    conn.on('ready', () => {
      conn.exec(`expect ${scriptPath}`, (err, stream) => {
        if (err) {
          conn.end();
          return reject(new Error(`Error ejecutando el script: ${err.message}`));
        }

        let output = '';

        stream.on('data', (data) => {
          output += data.toString();
        });

        stream.stderr.on('data', (data) => {
          output += data.toString();
        });

        stream.on('close', (code) => {
          console.log('[SSH] Stream finalizado. Código de salida:', code);
          conn.end();
          if (code === 0) {
            resolve({ code, output });
          } else {
            reject(new Error(`Script finalizó con código ${code}. Salida: ${output}`));
          }
        });
      });
    });

    conn.on('close', () => {
      console.log('[SSH] Conexión SSH cerrada correctamente');
    });

    conn.on('error', (err) => {
      console.error('[SSH] Error en la conexión SSH:', err.message);
      reject(err);
    });

    //conn.on('error', (err) => reject(err));
    /*conn.connect({
            host: process.env.SSH_HOST,
            port: process.env.SSH_PORT,
            username: process.env.SSH_USER,
            password: process.env.SSH_PASSWORD
        });
        /*/
    conn.connect({ host, port, username, password });
  });
}

module.exports = { ejecutarScriptRemoto };
