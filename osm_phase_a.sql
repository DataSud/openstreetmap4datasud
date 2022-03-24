create or replace function logfile(v_message text) returns integer as $$
declare
  v_ret integer := 0;
begin 
  raise notice '% : %', now(), v_message;
  return v_ret;
end;
$$ language plpgsql;

select
	logfile('partie A : préparation des traitements');
-----------------------------------------------------------------------------------------------
-- RESUME : Ce script SQL défini quatre fonctions qui créent de vues matérialisées à partir
--		des tables habillage_osm_*** :  
-- 	1. Création des vues matérialisées d'habillage
-- 	2. Création des tables des relations
--	3. Suppression des tables d'habillage inutiles				
--	4. Définition des fonctions de découpage et filtrage des thématique pour la region
--	5. Modification des noms d'attributs contenant des ":"
--	6. Import de la table des parcelles depuis vitis
-----------------------------------------------------------------------------------------------

---------------------------------------------------------------------
select logFile('A1 : Création des vues matérialisées d''habillage');
---------------------------------------------------------------------

select logFile('A1a : Création de la vue matérialisée des points (habillage_osm_point)');
-----------------------------------------------------------------------------------------
drop materialized view if exists global.mv_habillage_osm_point cascade;
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
--create unique index on global.mv_habillage_osm_point (osm_id);
comment on column global.mv_habillage_osm_point.natural_ is 'tag OSM original : natural'; --ajout d'un commentaire sur la colonne natural
--select * from global.mv_habillage_osm_point;

select logFile('A1b : Création de la vue matérialisée des polygones (habillage_osm_polygon)');
-----------------------------------------------------------------------------------------------
drop materialized view if exists global.mv_habillage_osm_polygon cascade;
create materialized view global.mv_habillage_osm_polygon as
	select distinct on (osm_id)
		osm_id, aeroway, amenity, barrier, boundary, building, craft, emergency, healthcare, highway, historic
		, landuse, leisure, man_made, military, "natural" as natural_, office, place, power, public_transport, railway, route
		, shop, sport, tourism, waterway, access, area, bridge, culvert, cutting, embankment, int_ref, layer
		, loc_ref, name, oneway, ref, service, tunnel, type, usage, water
	-- Métadonnées
		, source, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, z_order, way_area
		, now() as date_import, now()::date - osm_timestamp::date as lastmodif
		, array_length(akeys(tags), 1) as num_keys, tags
		, st_area(way) as superficie,tags->'building:part' as building_part
		, way as the_geom
	from public.habillage_osm_polygon;
--create unique index on global.mv_habillage_osm_polygon (osm_id);
comment on column global.mv_habillage_osm_polygon.natural_ is 'tag OSM original : natural'; --ajout d'un commentaire sur la colonne natural
--select * from global.mv_habillage_osm_polygon;

select logFile('A1c : Création de la vue matérialisée des lignes (habillage_osm_line)');
-----------------------------------------------------------------------------------------
drop materialized view if exists global.mv_habillage_osm_line cascade;
create materialized view global.mv_habillage_osm_line as
	select distinct on (osm_id)
		osm_id, aeroway, amenity, barrier, boundary, building, craft, emergency, healthcare, highway, historic, landuse
		, leisure, man_made, military, "natural" as natural_, office, place, power, public_transport, railway, route, shop, sport
		, tourism, waterway, access, area, bridge, culvert, cutting, embankment, int_ref, layer, loc_ref, name
		, oneway, ref, service, tunnel, type, usage, water
	-- Métadonnées
		, source, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, z_order, way_area
		, now() as date_import, now()::date - osm_timestamp::date as lastmodif
		, array_length(akeys(tags), 1) as num_keys, tags
		,st_length(way) as longueur
		, way as the_geom
	from public.habillage_osm_line --order by osm_id

	union
	
	select distinct on (osm_id)
		osm_id, aeroway, amenity, barrier, boundary, building, craft, emergency, healthcare, highway, historic, landuse
		, leisure, man_made, military, "natural" as natural_, office, place, power, public_transport, railway, route, shop, sport
		, tourism, waterway, access, area, bridge, culvert, cutting, embankment, int_ref, layer, loc_ref, name
		, oneway, ref, service, tunnel, type, usage, water
	-- Métadonnées
		, source, osm_user, osm_uid, osm_version, osm_changeset, osm_timestamp, z_order, way_area
		, now() as date_import, now()::date - osm_timestamp::date as lastmodif
		, array_length(akeys(tags), 1) as num_keys, tags
		,st_length(way) as longueur
		, st_exteriorring(way) as the_geom
	from public.habillage_osm_polygon
	where (highway not in ('services') and highway is not null and (area is null or area in ('no'))) and not exists (select osm_id from public.habillage_osm_line) order by osm_id
	;
--create unique index on global.mv_habillage_osm_line (osm_id);
comment on column global.mv_habillage_osm_line.natural_ is 'tag OSM original : natural'; --ajout d'un commentaire sur la colonne natural
--select * from global.mv_habillage_osm_line;

---------------------------------------------------------------------------------------------------
select logFile('A2 : Création des vues matérialisées des relations');
---------------------------------------------------------------------------------------------------

select logFile('A2a : Création de la vue matérialisée des relations (attributs - habillage_osm_rels)');
--------------------------------------------------------------------------------------------------------
drop materialized view if exists global.mv_habillage_osm_rels cascade;
create materialized view global.mv_habillage_osm_rels as
	select distinct on (id)
		id, way_off, rel_off, parts, members, tags
	-- Métadonnées		
		,tags::hstore as keys,members::hstore as mem_rels,array_length(akeys(tags::hstore), 1) as num_keys
		,array_length(akeys(members::hstore), 1) as num_mem, now() as date_import
	from public.habillage_osm_rels;
--create unique index on global.mv_habillage_osm_polygon (osm_id);
comment on column global.mv_habillage_osm_polygon.natural_ is 'tag OSM original : natural'; --ajout d'un commentaire sur la colonne natural
--select * from global.mv_habillage_osm_polygon;

select logFile('A2b : Création de la vue matérialisée des membres des relations (attributs)');
-----------------------------------------------------------------------------------------------
drop materialized view if exists global.mv_habillage_osm_membres_rel cascade;
create materialized view global.mv_habillage_osm_membres_rel as
	select distinct on (id) id, substr((each(mem_rels)).key, 1, 1) as primitive, substr((each(mem_rels)).key,2) as id_membre
		, (each(mem_rels)).key as membre, (each(mem_rels)).value as role, date_import, num_mem, keys->'type' as type
		, keys->'name' as nom, keys, num_keys
	from global.mv_habillage_osm_rels;
-- select * from global.mv_habillage_osm_rels

select logFile('A2c : Création de la vue matérialisée des clés des relations (attributs)');
--------------------------------------------------------------------------------------------
drop materialized view if exists global.mv_habillage_osm_cles_rel cascade;
create materialized view global.mv_habillage_osm_cles_rel as
	select distinct on (id) id, date_import, keys->'type' as type, keys->'name' as nom, keys, num_keys
	from global.mv_habillage_osm_rels;
--select * from global.mv_habillage_osm_rels;

select logFile('A2d : Création de la vue matérialisée des objets des relations');
--------------------------------------------------------------------------------------------
--lien objets géographiques <=> relations : points
drop materialized view if exists global.mv_habillage_objets_rel_point cascade;
create materialized view global.mv_habillage_objets_rel_point as
	select r.id as id_rel, r.primitive, r.id_membre, r.membre, r.role, r.num_mem, r.type as type_rel, r.nom, r.keys, r.num_keys as num_keys_rel,o.* 
	from global.mv_habillage_osm_membres_rel r 
	join global.mv_habillage_osm_point o on id_membre = osm_id::text
;

--lien objets géographiques <=> relations : lignes
drop materialized view if exists global.mv_habillage_objets_rel_line cascade;
create materialized view global.mv_habillage_objets_rel_line as
select r.id as id_rel, r.primitive, r.id_membre, r.membre, r.role, r.num_mem, r.type as type_rel, r.nom, r.keys, r.num_keys as num_keys_rel,o.* 
from global.mv_habillage_osm_membres_rel r
join global.mv_habillage_osm_line o on id_membre = osm_id::text
;

--lien objets géographiques <=> relations : lignes
drop materialized view if exists global.mv_habillage_objets_rel_polygon cascade;
create materialized view global.mv_habillage_objets_rel_polygon as
select r.id as id_rel, r.primitive, r.id_membre, r.membre, r.role, r.num_mem, r.type as type_rel, r.nom, r.keys, r.num_keys as num_keys_rel,o.* 
from global.mv_habillage_osm_membres_rel r
join global.mv_habillage_osm_polygon o on id_membre = osm_id::text
;

-------------------------------------------------------------------------------
select logFile('A3 Renommage des colonnes des tables d''habillage');
------------------------------------------------------------------------------
-- Fonction utilisée pour remplacer les ":" (colons) par des "_" (underscores)
-- dans le nom des colonnes

select logFile('A3a Création de la fonction de suppression des deux-points');
-------------------------------------------------------------------------------
drop function if exists decolonise(varchar,varchar);
create or replace function decolonise(nschema varchar, ntable varchar)
returns setof text AS $$
declare
	nom_col varchar;
	sql text;
	nvlle_val varchar;
	colon char := ':';
	underscore char := '_';
	resultat integer;
begin
	for nom_col in (select a.attname from pg_attribute a join pg_class t on a.attrelid=t.oid  join pg_namespace n on 
		t.relnamespace=n.oid where attname like '%:%' and nspname=$1 and t.relname=$2)
	loop
		select replace(nom_col,colon,underscore) into nvlle_val ;
		sql := 'alter table '||quote_ident($1)||'.'||quote_ident($2)||' 
				rename column '||quote_ident(nom_col)||' TO '||quote_ident(nvlle_val)||';
				comment on column '||quote_ident($1)||'.'||quote_ident($2)||'.'||quote_ident(nvlle_val)||' 
					is ''tag OSM original : '||quote_ident(nom_col)||''';';
		execute sql;
	end loop;
end
$$ language 'plpgsql';
comment on function decolonise(varchar,varchar) 
	is 'fonction pour renommer les colonnes d''une table qui contient deux points, remplacés par des underscore';

select logFile('A3b Lancements de la fonction de renommage des colonnes');
--------------------------------------------------------------------------
select decolonise('global','mv_habillage_osm_point');
select decolonise('global','mv_habillage_osm_line');
select decolonise('global','mv_habillage_osm_polygone');
select decolonise('global','mv_habillage_osm_cles_rel');
select decolonise('global','mv_habillage_osm_membres_rel');
select decolonise('global','mv_habillage_osm_rels');

select logFile('A4 FIN DES TRAITEMENTS POUR LA PARTIE A');
-----------------------------------------------------------
