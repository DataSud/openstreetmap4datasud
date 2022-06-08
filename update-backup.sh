#!/bin/bash
# source : https://github.com/springmeyer/up-to-date/blob/master/how_to.txt
# make sure osmososis/osm2pgsql are on your PATH
# cron does not inherit from your env
export PATH=/usr/bin/:$PATH
osmosis --read-replication-interval workingDirectory=. --simplify-change --write-xml-change latest.osc.gz 
osm2pgsql -r xml -s -C 300 --slim --create -C 1500 --number-processes 4 -E 2154 -j --extra-attributes -v  -S /home/user1/osmfiles/osmfiles_script/region_osm2pgsql.style  -d osmdiff -H 10.1.29.251 -U osm_user 
