module.exports = {
  apps : [{
    name: 'Plot Server',
    script: 'plot_server.py',

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    instances: 1,
    autorestart: true,
    watch: false,
    cwd: ".",
  }],
};
