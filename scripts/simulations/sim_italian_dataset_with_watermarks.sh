#!/usr/bin/env bash

python -m SmartCache.sim.utils compile --release "true" &&

simulator sim ../../config/simulations/italian_dataset_with_watermarks/lru.yml &&
simulator sim ../../config/simulations/italian_dataset_with_watermarks/lfu.yml &&
simulator sim ../../config/simulations/italian_dataset_with_watermarks/sizeBig.yml &&
simulator sim ../../config/simulations/italian_dataset_with_watermarks/sizeSmall.yml &&

simulator sim ../../config/simulations/italian_dataset_with_watermarks/scdl.yml &&

### ----- Plot results -----

python -m SmartCache.sim.simulator plot ../../results/sim_italian_dataset_with_watermarks --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_italian_dataset_with_watermarks/cache_100T_10Gbit/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_italian_dataset_with_watermarks/cache_100T_10Gbit/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &