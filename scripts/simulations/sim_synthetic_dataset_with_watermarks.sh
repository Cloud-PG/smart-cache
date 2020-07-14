#!/usr/bin/env bash

python -m SmartCache.sim.simulator sim ../../datasets/HighFrequencyDataset/ --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 10 --cache-size-unit "G" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermarks/HighFrequencyDataset --watermarks 'true' &&

python -m SmartCache.sim.simulator sim ../../datasets/SizeFocusedDataset/ --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 10 --cache-size-unit "G" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermarks/SizeFocusedDataset --watermarks 'true' &&

python -m SmartCache.sim.simulator sim ../../datasets/RecencyFocusedDataset/ --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 10 --cache-size-unit "G" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermarks/RecencyFocusedDataset --watermarks 'true' &&

# ------------------------ RL HighFrequencyDataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/HighFrequencyDataset --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 10 --cache-size-unit "G" --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermarks/HighFrequencyDataset --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.0005 &&

# ------------------------ RL SizeFocusedDataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/SizeFocusedDataset --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 10 --cache-size-unit "G" --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermarks/SizeFocusedDataset --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.0005 &&

# ------------------------ RL RecencyFocusedDataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/RecencyFocusedDataset --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 10 --cache-size-unit "G" --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermarks/RecencyFocusedDataset --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.0005 &&

### ----- Plot results -----

python -m SmartCache.sim.simulator plot ../../results/sim_syntethic_dataset_with_watermarks/HighFrequencyDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true' &&

python -m SmartCache.sim.simulator plot ../../results/sim_syntethic_dataset_with_watermarks/SizeFocusedDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true' &&

python -m SmartCache.sim.simulator plot ../../results/sim_syntethic_dataset_with_watermarks/RecencyFocusedDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_syntethic_dataset_with_watermarks/HighFrequencyDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_syntethic_dataset_with_watermarks/HighFrequencyDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &&

# python -m Probe.qTable ../../results/sim_syntethic_dataset_with_watermarks/SizeFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_syntethic_dataset_with_watermarks/SizeFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &&

# python -m Probe.qTable ../../results/sim_syntethic_dataset_with_watermarks/RecencyFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_syntethic_dataset_with_watermarks/RecencyFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv

