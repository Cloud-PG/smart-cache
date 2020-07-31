#!/usr/bin/env bash

python -m SmartCache.sim.utils compile &&

simulator sim ../../config/simulations/italian_dataset/lru.yml &&
simulator sim ../../config/simulations/italian_dataset/lfu.yml &&
simulator sim ../../config/simulations/italian_dataset/sizeBig.yml &&
simulator sim ../../config/simulations/italian_dataset/sizeSmall.yml &&

simulator sim ../../config/simulations/italian_dataset/scdl.yml &&

### ----- Plot results -----

python -m SmartCache.sim.simulator plot ../../results/sim_italian_dataset/cache_100T_10Gbit --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_italian_dataset/cache_100T_10Gbit/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_italian_dataset/cache_100T_10Gbit/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &