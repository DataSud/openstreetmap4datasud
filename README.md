# openstreetmap4datasud
Publication of OpenStreetmap data service in the DataSUD portal of Région Provence-Alpes-Côte d'Azur

Documentation sur le process de publication des données OpenStreetMap de la Région Provence-Alpes-Côte d'Azur sur le portail DataSUD.

## Mise en place initiale des données

1) Au préalable de la mise à jour des données OSM, il faut charger la base de données une première fois en utilisant l'extraction via l'url suivante : http://download.openstreetmap.fr/extracts/europe/france.osm.pbf (ou seulement la région souhaitée et ajouter les régions limitrophes).
Dans l'exemple suivant, nous allons créer une série de 7 tables préfixée par "habillage_osm_" dans le schéma public de PostGreSQL
```
osm2pgsql --slim --create -C 1500 --number-processes 4 /[mon_chemin]/[mon_fichier].osm -p habillage_osm -H [nom_de_l_hote] -P 5432 -E 2154 -j -d [ma_base] -U [nom_de_l_utilisateur] --extra-attributes -v -S ../osm2pgsql.style
```

2) Il faut lancer osm2pgsql une première fois pour créer les tables dans la base de données PostGreSQL.

3) Ensuite, il faut créer un dossier de travail (par exemple working_dir) et lui donner les droits d'accès.

4) Dans working_dir, il faut ensuite créer un dossier par région chargée et, dans chaque dossier, créer deux fichiers:
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
    osm2pgsql --append -r xml -s -C 300 --slim --number-processes 4 -E 2154 -p habillage_osm -j --extra-attributes -v  -S ../osm2pgsql.style -d [ma_base] -H [nom_de_l_hote] -U [nom_de_l_utilisateur] change.osc
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

### Tables d'habillage
Pour cela, il faut utiliser le fichier osm_phase_a.sql

Les tables habillage_osm_* sont un assemblage des régions qui ont été téléchargées et peuvent être utilisées pour réaliser des plans pour lesquels nous ne souhaitons pas que les données d'arrêtent à la limite de la région. Nous pouvons donc ajouter quelques informations supplémentaires en créant des vues matérialisées dans le schéma "global" à partir des tables originales. 

tables public.habillage_osm_* => vues matérialisées global.mv_habillage_osm_* 

Nous allons ajouter dans les tables : 
- la date de l'import : now() as date_import
- le nombre de jour écoulé depuis la dernière modification de l'objet : now()::date - osm_timestamp::date as lastmodif
- le nombre de clés présentes dans l'objet : array_length(akeys(tags), 1) as num_keys
- les coordonnées de l'objet : st_x(way) as coordx,st_y(way) as coordy
- ou sa longueur : st_length(way) as longueur
- ou sa surface : st_area(way) as superficie

Exemple avec la table des points :

```
create materialized view global.mv_habillage_osm_point as
	select distinct on (osm_id)
		osm_id, aeroway, amenity, barrier, boundary, building, craft, emergency, healthcare, highway, historic
		, landuse, leisure, man_made, military, "natural" as natural_, office, place, power, public_transport
		, railway, route, shop, sport, tourism, waterway, access, area, bridge, culvert, cutting, ele, embankment
		, int_ref, layer, loc_ref, name, oneway, ref, service, tunnel, type, usage, water
	-- Métadonnées
		, source, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, z_order
		, now() as date_import, now()::date - osm_timestamp::date as lastmodif
		, array_length(akeys(tags), 1) as num_keys, tags
		, st_x(way) as coordx,st_y(way) as coordy
		, way as the_geom
	from public.habillage_osm_point order by osm_id;
```

### Tables globales

Pour cela, il faut utiliser le fichier osm_phase_b.sql

Avant de créer les tables pour chaque thématiques, on va sélectionner les données qui intersectent l'emprise régionale. Il y a une particularité avec la tables des points car nous allons supprimer de la sélection les objets qui n'ont pas de "tags" (hors tags de métadonnées) pour alléger la table.

```
create materialized view theme.mv_region_point as(
	with pr as (
		select p.* from global.mv_habillage_osm_point p, administratif.region_sud a
		where ST_Intersects(a.geom, p.the_geom) order by osm_id)
	, pann as (select osm_id from pr where num_keys > 5 order by osm_id)
	select * from pr where osm_id in (select osm_id from pann) order by osm_id
);
```

### Tables thématiques

Chaque thématique peut, si besoin, être déclinée en points, lignes ou polygones. On peut également imaginer de créer des attributs supplémentaires. Ici, par exemple pour les tronçons de route, on ajoute un attribut "categorie" en fonction de l'importance de la route.

```
create materialized view theme.mv_highway_line as (
	select p.osm_id,highway,p.name,oneway,access,bridge,tunnel,layer,cutting,embankment,ref,loc_ref,int_ref
		,(case when highway like any (array['%motorway%','%trunk%','%primary%','%secondary%','%tertiary%']) then 1
				when highway like any (array['%residential%','%road%','%unclassified%']) then 2
				else 3 end) as categorie		
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and highway is not null and a.code_insee = '93'
);
```

## Mise à jour quotidienne des données

Pour cela, il faut utiliser le fichier osm_phase_c.sql

Ce script permet de rafraichir toutes les vues matérialisées

```
create or replace function theme.maj_vuesmat(schema_arg text default 'global')
returns int as $$
declare
    r record;
begin
    raise notice 'mise à jour des vues matérialisées du schéma %', schema_arg;
    for r in select matviewname from pg_matviews where schemaname = schema_arg order by matviewname
    loop
        raise notice 'mise à jour de %.%', schema_arg, r.matviewname;
        execute 'refresh materialized view ' || schema_arg || '.' || r.matviewname; 
    end loop;

    return 1;
end 
$$ language plpgsql;

select theme.maj_vuesmat('analyses');
select theme.maj_vuesmat('global');
select theme.maj_vuesmat('theme');
```




