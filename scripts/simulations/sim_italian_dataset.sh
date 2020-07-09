#!/usr/bin/env bash

python -m SmartCache.sim.simulator sim ../../datasets/source2018_numeric_it_with_avro_order --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 100 --cache-size-unit "T" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-bandwidth 10 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_italian_dataset/cache_100T_10Gbit &&

# ------------------------ RL italian dataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/source2018_numeric_it_with_avro_order --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 100 --cache-size-unit "T" --cache-bandwidth 10 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_italian_dataset/cache_100T_10Gbit --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.000005 &&

### ----- Plot results -----

python -m SmartCache.sim.simulator plot ../../results/sim_italian_dataset/cache_100T_10Gbit --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_italian_dataset/cache_100T_10Gbit/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_italian_dataset/cache_100T_10Gbit/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &