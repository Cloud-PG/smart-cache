#!/usr/bin/env bash

echo "===> $0" &&
python -m SmartCache.sim.utils compile --release "true" &&

simulator sim ../../config/simulations/italian_dataset_no_k/50T/lru.yml &&
simulator sim ../../config/simulations/italian_dataset_no_k/50T/lfu.yml &&
simulator sim ../../config/simulations/italian_dataset_no_k/50T/sizeBig.yml &&
simulator sim ../../config/simulations/italian_dataset_no_k/50T/sizeSmall.yml &&

simulator sim ../../config/simulations/italian_dataset_no_k/50T/scdl2.yml

### ----- Plot results -----

# python -m SmartCache.sim.simulator plot ../../results/sim_italian_dataset_no_k/50T --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_italian_dataset_no_k/50T/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_italian_dataset_no_k/50T/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &