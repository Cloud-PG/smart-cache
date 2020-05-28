#!/usr/bin/env bash

cargo build --release && cp target/release/libstats_mod.dylib stats_mod.so