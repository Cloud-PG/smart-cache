#!/usr/bin/env bash

python -m SmartCache.sim.utils compile --release "true" &&

# High Frequency Datset
simulator sim ../../config/simulations/synthetic_dataset/high_frequency_dataset/lru.yml &&
simulator sim ../../config/simulations/synthetic_dataset/high_frequency_dataset/lfu.yml &&
simulator sim ../../config/simulations/synthetic_dataset/high_frequency_dataset/sizeBig.yml &&
simulator sim ../../config/simulations/synthetic_dataset/high_frequency_dataset/sizeSmall.yml &&
simulator sim ../../config/simulations/synthetic_dataset/high_frequency_dataset/scdl.yml &&

# Recency Focused Datset
simulator sim ../../config/simulations/synthetic_dataset/recency_focused_dataset/lru.yml &&
simulator sim ../../config/simulations/synthetic_dataset/recency_focused_dataset/lfu.yml &&
simulator sim ../../config/simulations/synthetic_dataset/recency_focused_dataset/sizeBig.yml &&
simulator sim ../../config/simulations/synthetic_dataset/recency_focused_dataset/sizeSmall.yml &&
simulator sim ../../config/simulations/synthetic_dataset/recency_focused_dataset/scdl.yml &&

# Size Focused Datset
simulator sim ../../config/simulations/synthetic_dataset/size_focused_dataset/lru.yml &&
simulator sim ../../config/simulations/synthetic_dataset/size_focused_dataset/lfu.yml &&
simulator sim ../../config/simulations/synthetic_dataset/size_focused_dataset/sizeBig.yml &&
simulator sim ../../config/simulations/synthetic_dataset/size_focused_dataset/sizeSmall.yml &&
simulator sim ../../config/simulations/synthetic_dataset/size_focused_dataset/scdl.yml &&

### ----- Plot results -----

python -m SmartCache.sim.simulator plot ../../results/sim_synthetic_dataset/HighFrequencyDataset --plot-resolution 1280,800 --cache-bandwidth 10 --export-table 'true' &&

python -m SmartCache.sim.simulator plot ../../results/sim_synthetic_dataset/SizeFocusedDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true' &&

python -m SmartCache.sim.simulator plot ../../results/sim_synthetic_dataset/RecencyFocusedDataset --plot-resolution 1280,800 --cache-bandwidth 1 --export-table 'true'

### ----- Plot tables -----

# python -m Probe.qTable ../../results/sim_synthetic_dataset/HighFrequencyDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_synthetic_dataset/HighFrequencyDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &&

# python -m Probe.qTable ../../results/sim_synthetic_dataset/SizeFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_synthetic_dataset/SizeFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv &&

# python -m Probe.qTable ../../results/sim_synthetic_dataset/RecencyFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_additionQtable.csv &&

# python -m Probe.qTable ../../results/sim_synthetic_dataset/RecencyFocusedDataset/run_full_normal/aiRL_10G_it/aiRL_10G_it_evictionQtable.csv
