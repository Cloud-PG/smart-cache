module.exports = {
  apps : [{
    name: 'goCacheSim IT 10T',
    script: 'goCacheSim IT',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5431 --size 10485760 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim IT 100T',
    script: 'goCacheSim IT',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5432 --size 104857600 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim IT 200T',
    script: 'goCacheSim IT',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5433 --size 209715200 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim IT 10T LRU',
    script: 'goCacheSim IT',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5531 --size 10485760",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim IT 100T LRU',
    script: 'goCacheSim IT',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5532 --size 104857600",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim IT 200T LRU',
    script: 'goCacheSim IT',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5533 --size 209715200",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim US 10T',
    script: 'goCacheSim US',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5631 --size 10485760 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim US 100T',
    script: 'goCacheSim US',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5632 --size 104857600 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim US 200T',
    script: 'goCacheSim US',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run weightedLRU --port 5633 --size 209715200 --weightFunction FuncWeightedRequests",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim US 10T LRU',
    script: 'goCacheSim US',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5731 --size 10485760",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim US 100T LRU',
    script: 'goCacheSim US',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5732 --size 104857600",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  },
  {
    name: 'goCacheSim US 200T LRU',
    script: 'goCacheSim US',
    interpreter: null,

    // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
    args: "run lru --port 5733 --size 209715200",
    instances: 1,
    autorestart: false,
    watch: false,
    cwd: ".",
  }],
};
