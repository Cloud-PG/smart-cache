module.exports = {
  apps : [{
    name: 'goCacheSim 10T',
    script: 'goCacheSim',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5431 --size 10485760 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim 100T',
    script: 'goCacheSim',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5432 --size 104857600 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim 200T',
    script: 'goCacheSim',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5433 --size 209715200 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim 10T LRU',
    script: 'goCacheSim',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5531 --size 10485760",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim 100T LRU',
    script: 'goCacheSim',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5532 --size 104857600",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim 200T LRU',
    script: 'goCacheSim',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5533 --size 209715200",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  }],
};
