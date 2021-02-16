#!/usr/bin/env bash

BASE_DIR=$1

echo $BASE_DIR

pushd $BASE_DIR

echo "Remove previous folders"
rm -rf {jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}
rm -rf {jan-mar,apr-jun,jul-sep,oct-dec}
rm -rf {jan-apr,may-aug,sep-dec}

mkdir -p {jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}

echo "Prepare period window jan"
find . -maxdepth 1 -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan/cur_file
echo "Prepare period window feb"
find . -maxdepth 1 -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./feb/cur_file
echo "Prepare period window mar"
find . -maxdepth 1 -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./mar/cur_file
echo "Prepare period window apr"
find . -maxdepth 1 -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr/cur_file
echo "Prepare period window may"
find . -maxdepth 1 -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may/cur_file
echo "Prepare period window jun"
find . -maxdepth 1 -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jun/cur_file
echo "Prepare period window jul"
find . -maxdepth 1 -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul/cur_file
echo "Prepare period window aug"
find . -maxdepth 1 -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./aug/cur_file
echo "Prepare period window sep"
find . -maxdepth 1 -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep/cur_file
echo "Prepare period window oct"
find . -maxdepth 1 -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct/cur_file
echo "Prepare period window nov"
find . -maxdepth 1 -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./nov/cur_file
echo "Prepare period window dec"
find . -maxdepth 1 -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./dec/cur_file

mkdir -p {jan-mar,apr-jun,jul-sep,oct-dec}

echo "Prepare period window jan-mar"
find . -maxdepth 1 -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-mar/cur_file
find . -maxdepth 1 -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-mar/cur_file
find . -maxdepth 1 -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-mar/cur_file

echo "Prepare period window apr-jun"
find . -maxdepth 1 -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr-jun/cur_file
find . -maxdepth 1 -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr-jun/cur_file
find . -maxdepth 1 -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr-jun/cur_file

echo "Prepare period window jul-sep"
find . -maxdepth 1 -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul-sep/cur_file
find . -maxdepth 1 -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul-sep/cur_file
find . -maxdepth 1 -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul-sep/cur_file

echo "Prepare period window oct-dec"
find . -maxdepth 1 -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct-dec/cur_file
find . -maxdepth 1 -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct-dec/cur_file
find . -maxdepth 1 -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct-dec/cur_file

mkdir -p {jan-apr,may-aug,sep-dec}

echo "Prepare period window jan-apr"
find . -maxdepth 1 -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/cur_file
find . -maxdepth 1 -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/cur_file
find . -maxdepth 1 -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/cur_file
find . -maxdepth 1 -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/cur_file

echo "Prepare period window may-aug"
find . -maxdepth 1 -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/cur_file
find . -maxdepth 1 -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/cur_file
find . -maxdepth 1 -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/cur_file
find . -maxdepth 1 -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/cur_file

echo "Prepare period window sep-dec"
find . -maxdepth 1 -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/cur_file
find . -maxdepth 1 -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/cur_file
find . -maxdepth 1 -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/cur_file
find . -maxdepth 1 -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/cur_file

popd
