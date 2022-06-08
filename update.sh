#!/bin/bash
# source : https://github.com/springmeyer/up-to-date/blob/master/how_to.txt
# make sure osmososis/osm2pgsql are on your PATH
# cron does not inherit from your env
export PATH=/usr/bin/:$PATH


array=( liguria piemonte auvergne rhone paca languedoc )
for i in "${array[@]}"
do
	cd /home/user1/neogeo_osm/osmosis/$i
    rm change.osc
    osmosis --read-replication-interval workingDirectory=. --simplify-change --write-xml-change file="change.osc"
    osm2pgsql --append -r xml -s -C 300 --slim --number-processes 4 -E 2154 -p habillage_osm -j --extra-attributes -v  -S ../region_osm2pgsql.style -d osmdiff -H postgresql.lab.datasud.fr -U osm_user change.osc
done


echo
echo ------------------------------------------------------
echo RefreshAllMaterializedViews
echo
psql -h postgresql.lab.datasud.fr -U osm_user -d osmdiff << EOF
SELECT RefreshAllMaterializedViews('global');
SELECT RefreshAllMaterializedViews('theme');
SELECT RefreshAllMaterializedViews('analyses');
SELECT RefreshAllMaterializedViews('sfe');
EOF

