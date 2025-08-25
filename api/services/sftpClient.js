// sftpClient.js
const { Client } = require('ssh2');
const path = require('path');

function conectarSFTP({ host, port, username, password }) {
  return new Promise((resolve, reject) => {
    const conn = new Client();

    conn.on('ready', () => {
      conn.sftp((err, sftp) => {
        if (err) {
          conn.end();
          reject(err);
        } else {
          resolve({ conn, sftp });
        }
      });
    });

    conn.on('error', (err) => {
      reject(err);
    });

    conn.connect({
      host,
      port,
      username,
      password,
    });
  });
}

// Exporta correctamente:
module.exports = conectarSFTP;
