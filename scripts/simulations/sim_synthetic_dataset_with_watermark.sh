#!/usr/bin/env bash

python -m SmartCache.sim.simulator sim ../../datasets/HighFrequencyDataset/ --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 10 --cache-size-unit "G" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-size 10 --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermark/HighFrequencyDataset --watermarks 'true' &&

python -m SmartCache.sim.simulator sim ../../datasets/SizeFocusedDataset/ --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 10 --cache-size-unit "G" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-size 10 --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermark/SizeFocusedDataset --watermarks 'true' &&

python -m SmartCache.sim.simulator sim ../../datasets/RecencyFocusedDataset/ --force-exe-build 'true' --cache-type lru,lfu,sizeSmall,sizeBig --cache-size 10 --cache-size-unit "G" --simulation-steps normal --window-start 0 --window-stop 52 --region it --cache-size 10 --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermark/RecencyFocusedDataset --watermarks 'true' &&

# ------------------------ RL HighFrequencyDataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/HighFrequencyDataset --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 10 --cache-size-unit "G" --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermark/HighFrequencyDataset --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.00001 &&

# ------------------------ RL SizeFocusedDataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/SizeFocusedDataset --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 10 --cache-size-unit "G" --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermark/SizeFocusedDataset --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.00001 &&

# ------------------------ RL RecencyFocusedDataset  ----------------------------
python -m SmartCache.sim.simulator simAI ../../datasets/RecencyFocusedDataset --force-exe-build 'true' --simulation-steps normal --window-start 0 --window-stop 52 --cache-type aiRL --region it --cache-size 10 --cache-size-unit "G" --cache-bandwidth 1 --cache-bandwidth-redirect 'true' --out-folder ../../results/sim_syntethic_dataset_with_watermark/RecencyFocusedDataset --ai-rl-addition-feature-map ../../featureMaps/rlAdditionFeatureMap.json --ai-rl-eviction-feature-map ../../featureMaps/rlEvictionFeatureMap.json --load-prev-normal-run 'false' --dump-files-and-stats 'false' --decay-rate-epsilon 0.00001 &&

# ### ----- Plot results -----

python -m SmartCache.sim.simulator plot ../../results/sim_syntethic_dataset_with_watermark/HighFrequencyDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true' &&

python -m SmartCache.sim.simulator plot ../../results/sim_syntethic_dataset_with_watermark/SizeFocusedDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true' &&

python -m SmartCache.sim.simulator plot ../../results/sim_syntethic_dataset_with_watermark/RecencyFocusedDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true'

###
