
mkdir paca
wget http://download.openstreetmap.fr/extracts/europe/france/provence_alpes_cote_d_azur-latest.osm.pbf -O paca/sud.osm.pbf 
mkdir languedoc
wget http://download.openstreetmap.fr/extracts/europe/france/languedoc_roussillon-latest.osm.pbf -O languedoc/lr.osm.pbf
mkdir auvergne
wget http://download.openstreetmap.fr/extracts/europe/france/auvergne-latest.osm.pbf -O auvergne/auvergne.osm.pbf 
mkdir rhone
wget http://download.openstreetmap.fr/extracts/europe/france/rhone_alpes-latest.osm.pbf -O rhone/ra.osm.pbf 
mkdir liguria
wget http://download.openstreetmap.fr/extracts/europe/italy/liguria-latest.osm.pbf -O liguria/ligurie.osm.pbf
mkdir piemonte
wget http://download.openstreetmap.fr/extracts/europe/italy/piemonte-latest.osm.pbf -O piemonte/piemont.osm.pbf

osmosis --rb paca/sud.osm.pbf --rb languedoc/lr.osm.pbf --rb auvergne/auvergne.osm.pbf --rb rhone/ra.osm.pbf --rb liguria/ligurie.osm.pbf --rb piemonte/piemont.osm.pbf --merge --merge --merge --merge --merge --wb regions.osm.pbf

#/usr/bin/osm2pgsql --slim --create -C 1500 --number-processes 4 /home/user1/osmfiles/osmfiles_data/region.osm -p habillage_osm -H 10.1.29.251 -P 5432 -E 2154 -j -d osmdiff -U osm_user --extra-attributes -v -S ../region_osm2pgsql.style
