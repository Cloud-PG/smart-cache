#!/usr/bin/env bash

BASE_DIR=$1

echo $BASE_DIR

pushd $BASE_DIR

mkdir -p {jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}

echo "Prepare period window jan"
find . -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan/
echo "Prepare period window feb"
find . -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./feb/
echo "Prepare period window mar"
find . -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./mar/
echo "Prepare period window apr"
find . -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr/
echo "Prepare period window may"
find . -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may/
echo "Prepare period window jun"
find . -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jun/
echo "Prepare period window jul"
find . -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul/
echo "Prepare period window aug"
find . -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./aug/
echo "Prepare period window sep"
find . -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep/
echo "Prepare period window oct"
find . -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct/
echo "Prepare period window nov"
find . -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./nov/
echo "Prepare period window dec"
find . -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./dec/

mkdir -p {jan-mar,apr-jun,jul-sep,oct-dec}

echo "Prepare period window jan-mar"
find . -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-mar/
find . -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-mar/
find . -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-mar/

echo "Prepare period window apr-jun"
find . -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr-jun/
find . -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr-jun/
find . -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./apr-jun/

echo "Prepare period window jul-sep"
find . -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul-sep/
find . -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul-sep/
find . -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jul-sep/

echo "Prepare period window oct-dec"
find . -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct-dec/
find . -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct-dec/
find . -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./oct-dec/

mkdir -p {jan-apr,may-aug,sep-dec}

echo "Prepare period window jan-apr"
find . -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/
find . -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/
find . -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/
find . -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./jan-apr/

echo "Prepare period window may-aug"
find . -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/
find . -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/
find . -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/
find . -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./may-aug/

echo "Prepare period window sep-dec"
find . -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/
find . -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/
find . -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/
find . -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) ./sep-dec/

popd
