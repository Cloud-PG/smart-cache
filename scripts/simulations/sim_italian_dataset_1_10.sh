#!/usr/bin/env bash

echo "===> $0" &&
python -m SmartCache.sim.utils compile --release "true" &&

simulator sim ../../config/simulations/italian_dataset/10T/lru.yml &&
simulator sim ../../config/simulations/italian_dataset/10T/lfu.yml &&
simulator sim ../../config/simulations/italian_dataset/10T/sizeBig.yml &&
simulator sim ../../config/simulations/italian_dataset/10T/sizeSmall.yml &&

simulator sim ../../config/simulations/italian_dataset/10T/scdl2.yml

### ----- Plot results -----

# python -m SmartCache.sim.simulator plot ../../results/sim_italian_dataset/10T --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_italian_dataset/10T/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_italian_dataset/10T/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &