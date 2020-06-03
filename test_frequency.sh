python3 training_new.py --batch_size 128 --out_dir 'results_generator_may31/frequency_47_54'  --low_watermark 0. --warm_up_steps_add 600 --warm_up_steps_evict 600 --purge_delta 8000 --time_span_add 47 --time_span_evict 56  --gamma 0.99 --use_target_model True --output_activation 'linear'  --decay_rate_add 0.00005 --decay_rate_evict 0.00001 --report_choices True --data "/home/ubuntu/dataset-generator/HighFrequencyDataset" --cache_size 10240 --bandwidth 1. --start_month 1 --end_month 4 --invalidated_search_frequency 1  --lr 0.0000001
python3 training_new.py --batch_size 128 --out_dir 'results_generator_may31/frequency_47_54'  --low_watermark 0. --warm_up_steps_add 600 --warm_up_steps_evict 600 --purge_delta 8000 --time_span_add 47 --time_span_evict 56  --gamma 0.99 --use_target_model True --output_activation 'linear'  --decay_rate_add 0.00005 --decay_rate_evict 0.00001 --report_choices True --data "/home/ubuntu/dataset-generator/HighFrequencyDataset" --cache_size 10240 --bandwidth 1. --start_month 1 --end_month 4 --load_weights_from_file True --invalidated_search_frequency 1  --lr 0.0000001
python3 training_new.py --out_dir 'results_generator_may31/frequency_47_57'  --low_watermark 0.  --output_activation 'linear' --eps_add_max 0. --eps_evict_max 0. --data "/home/ubuntu/dataset-generator/HighFrequencyDataset" --report_choices True  --cache_size 10240 --bandwidth 1. --start_month 5 --end_month 12 --load_weights_from_file True --test False
