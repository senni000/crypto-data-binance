module.exports = {
  apps: [
    {
      name: 'binance-ingest',
      script: './dist/index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        BINANCE_PROCESS_ROLE: 'ingest',
      },
    },
    {
      name: 'binance-aggregate',
      script: './dist/index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '2G',
      env: {
        NODE_ENV: 'production',
        BINANCE_PROCESS_ROLE: 'aggregate',
      },
    },
    //アラートは一旦無効化
    // {
    //   name: 'binance-alert',
    //   script: './dist/index.js',
    //   instances: 1,
    //   autorestart: true,
    //   watch: false,
    //   max_memory_restart: '256M',
    //   env: {
    //     NODE_ENV: 'production',
    //     BINANCE_PROCESS_ROLE: 'alert',
    //   },
    // },
  ],
};
