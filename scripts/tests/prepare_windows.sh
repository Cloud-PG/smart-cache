#!/usr/bin/env bash

BASE_DIR=$1

echo $BASE_DIR

pushd $BASE_DIR

mkdir -p {jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}

echo "Prepare period window jan"
find . -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan/)
echo "Prepare period window feb"
find . -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./feb/)
echo "Prepare period window mar"
find . -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./mar/)
echo "Prepare period window apr"
find . -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./apr/)
echo "Prepare period window may"
find . -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./may/)
echo "Prepare period window jun"
find . -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jun/)
echo "Prepare period window jul"
find . -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jul/)
echo "Prepare period window aug"
find . -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./aug/)
echo "Prepare period window sep"
find . -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./sep/)
echo "Prepare period window oct"
find . -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./oct/)
echo "Prepare period window nov"
find . -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./nov/)
echo "Prepare period window dec"
find . -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./dec/)

mkdir -p {jan-mar,apr-jun,jul-sep,oct-dec}

echo "Prepare period window jan-mar"
find . -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-mar/)
find . -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-mar/)
find . -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-mar/)

echo "Prepare period window apr-jun"
find . -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./apr-jun/)
find . -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./apr-jun/)
find . -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./apr-jun/)

echo "Prepare period window jul-sep"
find . -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jul-sep/)
find . -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jul-sep/)
find . -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jul-sep/)

echo "Prepare period window oct-dec"
find . -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./oct-dec/)
find . -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./oct-dec/)
find . -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./oct-dec/)

mkdir -p {jan-apr,may-aug,sep-dec}

echo "Prepare period window jan-apr"
find . -name "results*_2018-01-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-apr/)
find . -name "results*_2018-02-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-apr/)
find . -name "results*_2018-03-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-apr/)
find . -name "results*_2018-04-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./jan-apr/)

echo "Prepare period window may-aug"
find . -name "results*_2018-05-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./may-aug/)
find . -name "results*_2018-06-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./may-aug/)
find . -name "results*_2018-07-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./may-aug/)
find . -name "results*_2018-08-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./may-aug/)

echo "Prepare period window sep-dec"
find . -name "results*_2018-09-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./sep-dec/)
find . -name "results*_2018-10-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./sep-dec/)
find . -name "results*_2018-11-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./sep-dec/)
find . -name "results*_2018-12-*.csv*" | xargs -I cur_file ln -sf $(realpath cur_file) $(realpath ./sep-dec/)

popd
