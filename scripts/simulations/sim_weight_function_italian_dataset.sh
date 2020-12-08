#!/usr/bin/env bash

# echo "===> $0" &&
# python -m SmartCache.sim.utils compile --release "true" &&

# pushd ../../config/simulations/weight_function_italian_dataset/100T &&
# python make_configs.py
# popd &&

simulator sim ../../config/simulations/weight_function_italian_dataset/100T/lru.yml &&
  simulator sim ../../config/simulations/weight_function_italian_dataset/100T/lfu.yml &&
  simulator sim ../../config/simulations/weight_function_italian_dataset/100T/sizeBig.yml &&
  simulator sim ../../config/simulations/weight_function_italian_dataset/100T/sizeSmall.yml

### ----- Plot results -----

# # python -m SmartCache.sim.simulator plot ../../results/sim_weight_function_italian_dataset/100T --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_weight_function_italian_dataset/100T/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_weight_function_italian_dataset/100T/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &
