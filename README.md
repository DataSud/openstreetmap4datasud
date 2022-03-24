# openstreetmap4datasud
Publication of OpenStreetmap data service in the DataSUD portal of Région Provence-Alpes-Côte d'Azur

Documentation sur le process de publication des données OpenStreetMap de la Région Provence-Alpes-Côte d'Azur sur le portail DataSUD.

## Mise en place initiale des données

1) Au préalable de la mise à jour des données OSM, il faut charger la base de données une première fois en utilisant l'extraction via l'url suivante : http://download.openstreetmap.fr/extracts/europe/france.osm.pbf (et ajouter les régions limitrophes).

2) Ensuite, il faut créer un dossier de travail (par exemple working_dir) et lui donner les droits d'accès.

3) Dans working_dir, il faut ensuite créer un dossier par région chargée et, dans chaque dossier, créer deux fichiers:
- states.txt en copiant tout ce qui se trouve dans (http://download.openstreetmap.fr/extracts/europe/france/[ma_region]/[ma_region].state.txt)
- configuration.txt en copiant le contenu suivant :

```
# The URL of the directory containing change files.
baseUrl=http://download.openstreetmap.fr/replication/europe/france/[ma_region]/minute/
# Defines the maximum time interval in seconds to download in a single invocation.
# Setting to 0 disables this feature.
maxInterval = 3600
#maxInterval = 0
```

4) Il faut ensuite copier le contenu suivant, de preference dans /home/nom_utilisateur/update.sh en vérifiant les droits d'accès également :

```
#!/bin/bash
# source : https://github.com/springmeyer/up-to-date/blob/master/how_to.txt
# make sure osmososis/osm2pgsql are on your PATH
# cron does not inherit from your env
export PATH=/usr/bin/:$PATH
# On va créer ici un dossier par région téléchargée (la région souhaitée + les régions limitrophes)
array=( liguria piemonte auvergne rhone paca languedoc )
for i in "${array[@]}"
do
	cd /home/nom_utilisateur/[mon_chemin]/$i
    rm change.osc
    osmosis --read-replication-interval workingDirectory=. --simplify-change --write-xml-change file="change.osc"
    osm2pgsql --append -r xml -s -C 300 --slim --number-processes 4 -E 2154 -p habillage_osm -j --extra-attributes -v  -S ../region_osm2pgsql.style -d osmdiff -H [nom_de_l_hote] -U [nom_de_l_utilisateur] change.osc
done

echo
echo ------------------------------------------------------
echo RefreshAllMaterializedViews
echo
psql -h 10.1.29.251 -U osm_user -d osmdiff << EOF
SELECT RefreshAllMaterializedViews('global');
SELECT RefreshAllMaterializedViews('theme');
SELECT RefreshAllMaterializedViews('analyses');
SELECT RefreshAllMaterializedViews('sfe');
EOF
```

5) Il faut rendre le fichier executable avec la commande chmod +x /[mon_chemin]/update_osm_db.sh

6) On exécute le fichier et, si tout ce passe bien, il ne reste plus qu’a configurer le lancement de ce fichier tous les jours avec le cron
(https://debian-facile.org/doc:systeme:crontab)
Exemple pour tous les jours a 6h : 06 * * * * /[mon_chemin]/update_osm_db.sh

## Création des view matérialisées
L'init

## Mise à jour quotidienne des données
La tâche cron mise en place dans la première partie va mettre à jour les données dans votre base de données PostGreSQL. Dans notre exemple, une série de tables préfixée par habillage_ sera crée dans le schéma public de PostGreSQL

A partir de ces tables, il va falloir 
1) créer des copies des tables d'habillage dans des vues matérialisées.
2) créer toutes les tables thématiques souhaitées en utilisant les vues matérialisées précédentes.
3) 

