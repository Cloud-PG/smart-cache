#!/usr/bin/env bash

echo "===> $0" &&
python -m SmartCache.sim.utils compile --release "true" &&

simulator sim ../../config/simulations/us_dataset/200T/lru.yml &&
simulator sim ../../config/simulations/us_dataset/200T/lfu.yml &&
simulator sim ../../config/simulations/us_dataset/200T/sizeBig.yml &&
simulator sim ../../config/simulations/us_dataset/200T/sizeSmall.yml &&

simulator sim ../../config/simulations/us_dataset/200T/scdl2.yml

### ----- Plot results -----

# python -m SmartCache.sim.simulator plot ../../results/sim_us_dataset/200T --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_us_dataset/200T/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_us_dataset/200T/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &