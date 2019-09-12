module.exports = {
    apps: [{
            name: 'goCacheSim Service',
            script: 'goCacheSim',
            interpreter: null,

            // Options reference: https://pm2.io/doc/en/runtime/reference/ecosystem-file/
            args: "serve weighted --port 4243 --size 10485760 --weightFunction FuncWeightedRequests",
            instances: 1,
            cwd: ".",
        },
        {
            name: 'goCacheSim Service Server',
            script: 'app.py',
            interpreter: "python3",
            instances: 1,
            cwd: "."
        }
    ]
}