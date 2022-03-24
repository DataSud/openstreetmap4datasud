select logFile('PARTIE B : Filtrage des données');
-------------------------------------------------------------------------------------------------------------------
-- RESUME : Lance une série de filtre permettant de créer les vues matérialisées concernant le territoire de la région.
--	Il créé les vues matérialisées génériques mv_region_**, thématiques ou spécifiques mv_theme_** et les analyses statistiques :
--		1. Création des vues matérialisées génériques
--		2. Création des vues matérialisées thématiques : mv_theme_**
--		3. Création des vues matérialisées spécifiques (adresses, voirie, géodésie, tourisme, activités)
--		4. Lancement des analyses statistiques (contributeurs, clés)
---------------------------------------------------------------------------------------------------

-------------------------------------------------------------------
select logFile('B1 : Création des vues matérialisées génériques');
-------------------------------------------------------------------

select logFile('B1a : Création de la table des points');
---------------------------------------------------------
-- Découpage, selon l'emprise de la region, de la table des points ayant au moins un attribut.
-- avec suppression des objets n'ayant pas de clé, en dehors des 5 clés de métadonnées.
drop materialized view if exists theme.mv_region_point cascade;
create materialized view theme.mv_region_point as(
	with pr as (
		select p.* from global.mv_habillage_osm_point p, administratif.region_sud a
		where ST_Intersects(a.geom, p.the_geom) order by osm_id)
	, pann as (select osm_id from pr where num_keys > 5 order by osm_id)
	select * from pr where osm_id in (select osm_id from pann) order by osm_id
);
create unique index index_mv_region_point on theme.mv_region_point (osm_id);

select logFile('B1b : Création de la table des lignes');
--------------------------------------------------------
-- Découpage de la table des lignes selon l'emprise de la region.
drop materialized view if exists theme.mv_region_line cascade;
create materialized view theme.mv_region_line as (
	select p.* from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom));
create unique index index_mv_region_line on theme.mv_region_line (osm_id);

select logFile('B1c : Création de la table des polygones');
-----------------------------------------------------------
-- Découpage de la table des polygones selon l'emprise de la region.
drop materialized view if exists theme.mv_region_polygon cascade;
create materialized view theme.mv_region_polygon as (
	select p.* from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom));
create unique index index_mv_region_polygon on theme.mv_region_polygon (osm_id);

-------------------------------------------------------------------
select logFile('B2 : Création des vues matérialisées thématiques');
-------------------------------------------------------------------

select logFile('B2-01 : Vues matérialisées aerialway');
--------------------------------------------------------
--aerialway
drop materialized view if exists theme.mv_aerialway_point cascade;
create materialized view theme.mv_aerialway_point as (
	select osm_id,tags->'aerialway' as aerialway,name
		,tags->'aerialway:occupancy' as occupancy,tags->'aerialway:capacity' as capacity
		,tags->',aerialway:duration' as duration,tags->'aerialway:detachable' as detachable
		,tags->',aerialway:bubble' as bubble,tags->'aerialway:heating' as heating
		,tags->'aerialway:bicycle' as bicycle,tags->'aerialway:access' as access
		,tags->'aerialway:summer:access' as summer_access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from theme.mv_region_point p where tags->'aerialway' is not null
);
create unique index index_mv_aerialway_point on theme.mv_aerialway_point (osm_id);

drop materialized view if exists theme.mv_aerialway_line cascade;
create materialized view theme.mv_aerialway_line as (
	select osm_id,tags->'aerialway' as aerialway,name
		,tags->'aerialway:occupancy' as occupancy,tags->'aerialway:capacity' as capacity
		,tags->',aerialway:duration' as duration,tags->'aerialway:detachable' as detachable
		,tags->',aerialway:bubble' as bubble,tags->'aerialway:heating' as heating
		,tags->'aerialway:bicycle' as bicycle,tags->'aerialway:access' as access
		,tags->'aerialway:summer:access' as summer_access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from theme.mv_region_line p where tags->'aerialway' is not null
);
create unique index index_mv_aerialway_line on theme.mv_aerialway_line (osm_id);

drop materialized view if exists theme.mv_aerialway_polygone cascade;
create materialized view theme.mv_aerialway_polygone as (
	select osm_id,tags->'aerialway' as aerialway,name
		,tags->'aerialway:occupancy' as occupancy,tags->'aerialway:capacity' as capacity
		,tags->',aerialway:duration' as duration,tags->'aerialway:detachable' as detachable
		,tags->',aerialway:bubble' as bubble,tags->'aerialway:heating' as heating
		,tags->'aerialway:bicycle' as bicycle,tags->'aerialway:access' as access
		,tags->'aerialway:summer:access' as summer_access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from theme.mv_region_polygon p where tags->'aerialway' is not null
);
create unique index index_mv_aerialway_polygone on theme.mv_aerialway_polygone (osm_id);

select logFile('B2-02 : Vues matérialisées aeroway');
--------------------------------------------------------
--aeroway
drop materialized view if exists theme.mv_aeroway_point cascade;
create materialized view theme.mv_aeroway_point as (
	select p.osm_id,p.aeroway,p.name,access,tags->'airmark' as airmark
		,tags->'iata' as iata,tags->'icao' as icao
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from theme.mv_region_point p where aeroway is not null
);
drop materialized view if exists theme.mv_aeroway_line cascade;
create materialized view theme.mv_aeroway_line as (
	select p.osm_id,p.aeroway,p.name,access,tags->'airmark' as airmark
		,tags->'iata' as iata,tags->'icao' as icao
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from theme.mv_region_line p where aeroway is not null
);
drop materialized view if exists theme.mv_aeroway_polygone cascade;
create materialized view theme.mv_aeroway_polygone as (
	select p.osm_id,p.aeroway,p.name,access,tags->'airmark' as airmark
		,tags->'iata' as iata,tags->'icao' as icao
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from theme.mv_region_polygon p where aeroway is not null
);

select logFile('B2-03 : Vues matérialisées amenity');
--------------------------------------------------------
--amenity
drop materialized view if exists theme.mv_amenity_point cascade;
create materialized view theme.mv_amenity_point as (
	select p.osm_id,p.amenity,p.name,access
		,(case when amenity in ('bar','biergarten','cafe','fast_food','food_court','ice_cream','pub','restaurant') 
					then 'nourriture'
				when amenity in ('college','driving_school','kindergarten','language_school','library','toy_library','music_school','school','university')
					then 'éducation'
				when amenity in ('kick-scooter_rental','bicycle_parking','bicycle_repair_station','bicycle_rental','boat_rental','boat_sharing','bus_station','car_rental','car_sharing','car_wash','vehicle_inspection','charging_station','ferry_terminal','fuel','grit_bin','motorcycle_parking','parking','parking_entrance','parking_entrance','taxi')
					then 'transports'
				when amenity in ('atm','bank','bureau_de_change')
					then 'service financier'
				when amenity in ('baby_hatch','clinic','dentist','doctors','hospital','nursing_home','pharmacy','social_facility','veterinary')
					then 'service de santé'
				when amenity in ('arts_centre','brothel','casino','cinema','community_centre','conference_centre','events_venue','fountain','gambling','love_hotel','nightclub','planetarium','public_bookcase','social_centre','stripclub','studio','swingerclub','theatre')
					then 'divertissement'
				when amenity in ('courthouse','fire_station','police','post_box','post_depot','post_office','prison','ranger_station','townhall')
					then 'service public'
				when amenity in ('bbq','bench','dog_toilet','drinking_water','give_box','freeshop','shelter','shower','telephone','toilets','water_point','watering_place')
					then 'équipement'
				when amenity in ('sanitary_dump_station','recycling','waste_basket','waste_disposal','waste_transfer_station')
					then 'déchets'
				else 'autre service ou équipement'
		end) as type_service
		,(case when amenity in ('bank','bar','bicycle_rental','bicycle_repair_station','cafe','car_rental','car_wash','childcare',
				 'cinema','cleanning','clinic','college','community_centre','dentist','doctors','driving_school',
				 'fast_food','financial_advice','fire_station','healthcare','hospital','hookah_lounge','ice_cream',
				 'kindergarten','marketplace','pharmacy','police','poste_office','restaurant','school',
				 'social_facility','taxi','vehicle_inspection','veterinary')
					then true else false 
		end) as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'service' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from theme.mv_region_point p where amenity is not null
);
drop materialized view if exists theme.mv_amenity_line cascade;
create materialized view theme.mv_amenity_line as (
	select p.osm_id,p.amenity,p.name,access
		,(case when amenity in ('bar','biergarten','cafe','fast_food','food_court','ice_cream','pub','restaurant') 
					then 'nourriture'
				when amenity in ('college','driving_school','kindergarten','language_school','library','toy_library','music_school','school','university')
					then 'éducation'
				when amenity in ('kick-scooter_rental','bicycle_parking','bicycle_repair_station','bicycle_rental','boat_rental','boat_sharing','bus_station','car_rental','car_sharing','car_wash','vehicle_inspection','charging_station','ferry_terminal','fuel','grit_bin','motorcycle_parking','parking','parking_entrance','parking_entrance','taxi')
					then 'transports'
				when amenity in ('atm','bank','bureau_de_change')
					then 'service financier'
				when amenity in ('baby_hatch','clinic','dentist','doctors','hospital','nursing_home','pharmacy','social_facility','veterinary')
					then 'service de santé'
				when amenity in ('arts_centre','brothel','casino','cinema','community_centre','conference_centre','events_venue','fountain','gambling','love_hotel','nightclub','planetarium','public_bookcase','social_centre','stripclub','studio','swingerclub','theatre')
					then 'divertissement'
				when amenity in ('courthouse','fire_station','police','post_box','post_depot','post_office','prison','ranger_station','townhall')
					then 'service public'
				when amenity in ('bbq','bench','dog_toilet','drinking_water','give_box','freeshop','shelter','shower','telephone','toilets','water_point','watering_place')
					then 'équipement'
				when amenity in ('sanitary_dump_station','recycling','waste_basket','waste_disposal','waste_transfer_station')
					then 'déchets'
				else 'autre service ou équipement'
		end) as type_service		
		,(case when amenity in ('bank','bar','bicycle_rental','bicycle_repair_station','cafe','car_rental','car_wash','childcare',
				 'cinema','cleanning','clinic','college','community_centre','dentist','doctors','driving_school',
				 'fast_food','financial_advice','fire_station','healthcare','hospital','hookah_lounge','ice_cream',
				 'kindergarten','marketplace','pharmacy','police','poste_office','restaurant','school',
				 'social_facility','taxi','vehicle_inspection','veterinary')
					then true else false 
		end) as activite_economique	
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'service' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from theme.mv_region_line p where amenity is not null
);
drop materialized view if exists theme.mv_amenity_polygone cascade;
create materialized view theme.mv_amenity_polygone as (
	select p.osm_id,p.amenity,p.name,access
		,(case when amenity in ('bar','biergarten','cafe','fast_food','food_court','ice_cream','pub','restaurant') 
					then 'nourriture'
				when amenity in ('college','driving_school','kindergarten','language_school','library','toy_library','music_school','school','university')
					then 'éducation'
				when amenity in ('kick-scooter_rental','bicycle_parking','bicycle_repair_station','bicycle_rental','boat_rental','boat_sharing','bus_station','car_rental','car_sharing','car_wash','vehicle_inspection','charging_station','ferry_terminal','fuel','grit_bin','motorcycle_parking','parking','parking_entrance','parking_entrance','taxi')
					then 'transports'
				when amenity in ('atm','bank','bureau_de_change')
					then 'service financier'
				when amenity in ('baby_hatch','clinic','dentist','doctors','hospital','nursing_home','pharmacy','social_facility','veterinary')
					then 'service de santé'
				when amenity in ('arts_centre','brothel','casino','cinema','community_centre','conference_centre','events_venue','fountain','gambling','love_hotel','nightclub','planetarium','public_bookcase','social_centre','stripclub','studio','swingerclub','theatre')
					then 'divertissement'
				when amenity in ('courthouse','fire_station','police','post_box','post_depot','post_office','prison','ranger_station','townhall')
					then 'service public'
				when amenity in ('bbq','bench','dog_toilet','drinking_water','give_box','freeshop','shelter','shower','telephone','toilets','water_point','watering_place')
					then 'équipement'
				when amenity in ('sanitary_dump_station','recycling','waste_basket','waste_disposal','waste_transfer_station')
					then 'déchets'
				else 'autre service ou équipement'
		end) as type_service
		,(case when amenity in ('bank','bar','bicycle_rental','bicycle_repair_station','cafe','car_rental','car_wash','childcare',
				 'cinema','cleanning','clinic','college','community_centre','dentist','doctors','driving_school',
				 'fast_food','financial_advice','fire_station','healthcare','hospital','hookah_lounge','ice_cream',
				 'kindergarten','marketplace','pharmacy','police','poste_office','restaurant','school',
				 'social_facility','taxi','vehicle_inspection','veterinary')
					then true else false 
		end) as activite_economique	
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'service' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from theme.mv_region_polygon p where amenity is not null
);

select logFile('B2-04 : Vues matérialisées barrier');
--------------------------------------------------------
--barrier
drop materialized view if exists theme.mv_barrier_point cascade;
create materialized view theme.mv_barrier_point as (
	select p.osm_id,p.barrier,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from theme.mv_region_point p where barrier is not null
);
drop materialized view if exists theme.mv_barrier_line cascade;
create materialized view theme.mv_barrier_line as (
	select p.osm_id,p.barrier,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from theme.mv_region_line p where barrier is not null
);
drop materialized view if exists theme.mv_barrier_polygone cascade;
create materialized view theme.mv_barrier_polygone as (
	select p.osm_id,p.barrier,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from theme.mv_region_polygon p where barrier is not null
);

select logFile('B2-05 : Vues matérialisées boundary');
--------------------------------------------------------
--boundary
drop materialized view if exists theme.mv_boundary_point cascade;
create materialized view theme.mv_boundary_point as (
	select p.osm_id,p.boundary,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and boundary is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_boundary_line cascade;
create materialized view theme.mv_boundary_line as (
	select p.osm_id,p.boundary,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and boundary is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_boundary_polygone cascade;
create materialized view theme.mv_boundary_polygone as (
	select p.osm_id,p.boundary,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and boundary is not null and a.code_insee = '93'
);

select logFile('B2-06 : Vues matérialisées building');
--------------------------------------------------------
--building et building_part
drop materialized view if exists theme.mv_building_point cascade;
create materialized view theme.mv_building_point as (
	select p.osm_id,p.building,tags->'building:part' as building_part,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and (building is not null or tags->'building:part' is not null) 
		and a.code_insee = '93'
);
drop materialized view if exists theme.mv_building_line cascade;
create materialized view theme.mv_building_line as (
select p.osm_id,p.building,tags->'building:part' as building_part,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and (building is not null or tags->'building:part' is not null) 
		and a.code_insee = '93'
);
drop materialized view if exists theme.mv_building_polygone cascade;
create materialized view theme.mv_building_polygone as (
	select p.osm_id,p.building,tags->'building:part' as building_part,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and (building is not null or tags->'building:part' is not null) 
		and a.code_insee = '93'
);

select logFile('B2-07 : Vues matérialisées craft');
--------------------------------------------------------
--craft
drop materialized view if exists theme.mv_craft_point cascade;
create materialized view theme.mv_craft_point as (
	select p.osm_id,p.craft,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'artisanat' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and craft is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_craft_line cascade;
create materialized view theme.mv_craft_line as (
	select p.osm_id,p.craft,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'artisanat' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and craft is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_craft_polygone cascade;
create materialized view theme.mv_craft_polygone as (
	select p.osm_id,p.craft,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'artisanat' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and craft is not null and a.code_insee = '93'
);

select logFile('B2-08 : Vues matérialisées emergency');
--------------------------------------------------------
--emergency
drop materialized view if exists theme.mv_emergency_point cascade;
create materialized view theme.mv_emergency_point as (
	select p.osm_id,p.emergency,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and emergency is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_emergency_line cascade;
create materialized view theme.mv_emergency_line as (
	select p.osm_id,p.emergency,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and emergency is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_emergency_polygone cascade;
create materialized view theme.mv_emergency_polygone as (
	select p.osm_id,p.emergency,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and emergency is not null and a.code_insee = '93'
);

select logFile('B2-09 : Vues matérialisées geological');
--------------------------------------------------------
--geological
drop materialized view if exists theme.mv_geological_point cascade;
create materialized view theme.mv_geological_point as (
	select p.osm_id,tags->'geological' as geological,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tags->'geological' is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_geological_line cascade;
create materialized view theme.mv_geological_line as (
	select p.osm_id,tags->'geological' as geological,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tags->'geological' is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_geological_polygone cascade;
create materialized view theme.mv_geological_polygone as (
	select p.osm_id,tags->'geological' as geological,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tags->'geological' is not null and a.code_insee = '93'
);

select logFile('B2-10 : Vues matérialisées healthcare');
--------------------------------------------------------
--healthcare
drop materialized view if exists theme.mv_healthcare_point cascade;
create materialized view theme.mv_healthcare_point as (
	select p.osm_id,healthcare,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and healthcare is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_healthcare_line cascade;
create materialized view theme.mv_healthcare_line as (
	select p.osm_id,healthcare,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and healthcare is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_healthcare_polygone cascade;
create materialized view theme.mv_healthcare_polygone as (
	select p.osm_id,healthcare,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and healthcare is not null and a.code_insee = '93'
);

select logFile('B2-10 : Vues matérialisées highway');
--------------------------------------------------------
--highway
drop materialized view if exists theme.mv_highway_point cascade;
create materialized view theme.mv_highway_point as (
	select p.osm_id,highway,p.name,oneway,access,bridge,tunnel,layer,cutting,embankment,ref,loc_ref,int_ref
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and highway is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_highway_line cascade;
create materialized view theme.mv_highway_line as (
	select p.osm_id,highway,p.name,oneway,access,bridge,tunnel,layer,cutting,embankment,ref,loc_ref,int_ref
		,(case when highway like any (array['%motorway%','%trunk%','%primary%','%secondary%','%tertiary%']) then 1
				when highway like any (array['%residential%','%road%','%unclassified%']) then 2
				else 3 end) as categorie		
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and highway is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_highway_polygone cascade;
create materialized view theme.mv_highway_polygone as (
	select p.osm_id,highway,p.name,oneway,access,bridge,tunnel,layer,cutting,embankment,ref,loc_ref,int_ref,area
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and highway is not null and a.code_insee = '93'
);

select logFile('B2-11 : Vues matérialisées historic');
--------------------------------------------------------
--historic
drop materialized view if exists theme.mv_historic_point cascade;
create materialized view theme.mv_historic_point as (
	select p.osm_id,historic,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and historic is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_historic_line cascade;
create materialized view theme.mv_historic_line as (
	select p.osm_id,historic,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and historic is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_historic_polygone cascade;
create materialized view theme.mv_historic_polygone as (
	select p.osm_id,historic,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and historic is not null and a.code_insee = '93'
);

select logFile('B2-12 : Vues matérialisées landuse');
--------------------------------------------------------
--landuse
drop materialized view if exists theme.mv_landuse_point cascade;
create materialized view theme.mv_landuse_point as (
	select p.osm_id,landuse,p.name,access,place
		,(case when landuse in ('commercial','industrial','orchard','plant_nursery','quarry','retail','tourism','vineyard')
			then true else false 
		end) as activite_economique		
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'autre' end) as type_activite
		,tags->'ref:FR:commune' as id_interne
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and landuse is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_landuse_line cascade;
create materialized view theme.mv_landuse_line as (
	select p.osm_id,landuse,p.name,access,place
		,(case when landuse in ('commercial','industrial','orchard','plant_nursery','quarry','retail','tourism','vineyard')
			then true else false 
		end) as activite_economique		
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'autre' end) as type_activite
		,tags->'ref:FR:commune' as id_interne
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and landuse is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_landuse_polygone cascade;
create materialized view theme.mv_landuse_polygone as (
	select p.osm_id,landuse,p.name,access,place
		,(case when landuse in ('commercial','industrial','orchard','plant_nursery','quarry','retail','tourism','vineyard')
			then true else false 
		end) as activite_economique		
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'autre' end) as type_activite
		,tags->'ref:FR:commune' as id_interne
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and landuse is not null and a.code_insee = '93'
);

select logFile('B2-13 : Vues matérialisées leisure');
--------------------------------------------------------
--leisure
drop materialized view if exists theme.mv_leisure_point cascade;
create materialized view theme.mv_leisure_point as (
	select p.osm_id,leisure,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and leisure is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_leisure_line cascade;
create materialized view theme.mv_leisure_line as (
	select p.osm_id,leisure,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and leisure is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_leisure_polygone cascade;
create materialized view theme.mv_leisure_polygone as (
	select p.osm_id,leisure,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and leisure is not null and a.code_insee = '93'
);

select logFile('B2-14 : Vues matérialisées man_made');
--------------------------------------------------------
--man_made
drop materialized view if exists theme.mv_man_made_point cascade;
create materialized view theme.mv_man_made_point as (
	select p.osm_id,man_made,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and man_made is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_man_made_line cascade;
create materialized view theme.mv_man_made_line as (
	select p.osm_id,man_made,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and man_made is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_man_made_polygone cascade;
create materialized view theme.mv_man_made_polygone as (
	select p.osm_id,man_made,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and man_made is not null and a.code_insee = '93'
);

select logFile('B2-15 : Vues matérialisées military');
--------------------------------------------------------
--military
drop materialized view if exists theme.mv_military_point cascade;
create materialized view theme.mv_military_point as (
	select p.osm_id,military,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and military is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_military_line cascade;
create materialized view theme.mv_military_line as (
	select p.osm_id,military,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and military is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_military_polygone cascade;
create materialized view theme.mv_military_polygone as (
	select p.osm_id,military,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and military is not null and a.code_insee = '93'
);

select logFile('B2-16 : Vues matérialisées natural');
--------------------------------------------------------
--natural
drop materialized view if exists theme.mv_natural_point cascade;
create materialized view theme.mv_natural_point as (
	select p.osm_id,natural_,p.name,access,water
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and natural_ is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_natural_line cascade;
create materialized view theme.mv_natural_line as (
	select p.osm_id,natural_,p.name,access,water
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and natural_ is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_natural_polygone cascade;
create materialized view theme.mv_natural_polygone as (
	select p.osm_id,natural_,p.name,access,water
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and natural_ is not null and a.code_insee = '93'
);

select logFile('B2-17 : Vues matérialisées office');
--------------------------------------------------------
--office
drop materialized view if exists theme.mv_office_point cascade;
create materialized view theme.mv_office_point as (
	select p.osm_id,office,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'bureau' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and office is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_office_line cascade;
create materialized view theme.mv_office_line as (
	select p.osm_id,office,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'bureau' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and office is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_office_polygone cascade;
create materialized view theme.mv_office_polygone as (
	select p.osm_id,office,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'bureau' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and office is not null and a.code_insee = '93'
);

select logFile('B2-18 : Vues matérialisées place');
--------------------------------------------------------
--place
drop materialized view if exists theme.mv_place_point cascade;
create materialized view theme.mv_place_point as (
	select p.osm_id,place,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and place is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_place_line cascade;
create materialized view theme.mv_place_line as (
	select p.osm_id,place,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and place is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_place_polygone cascade;
create materialized view theme.mv_place_polygone as (
	select p.osm_id,place,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and place is not null and a.code_insee = '93'
);

select logFile('B2-19 : Vues matérialisées power');
--------------------------------------------------------
--power
drop materialized view if exists theme.mv_power_point cascade;
create materialized view theme.mv_power_point as (
	select p.osm_id,power,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and power is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_power_line cascade;
create materialized view theme.mv_power_line as (
	select p.osm_id,power,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and power is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_power_polygone cascade;
create materialized view theme.mv_power_polygone as (
	select p.osm_id,power,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and power is not null and a.code_insee = '93'
);

select logFile('B2-20 : Vues matérialisées public_transport');
---------------------------------------------------------------
--public_transport
drop materialized view if exists theme.mv_public_transport_point cascade;
create materialized view theme.mv_public_transport_point as (
	select p.osm_id,public_transport,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and public_transport is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_public_transport_line cascade;
create materialized view theme.mv_public_transport_line as (
	select p.osm_id,public_transport,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and public_transport is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_public_transport_polygone cascade;
create materialized view theme.mv_public_transport_polygone as (
	select p.osm_id,public_transport,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and public_transport is not null and a.code_insee = '93'
);

select logFile('B2-21 : Vues matérialisées railway');
------------------------------------------------------
--railway
drop materialized view if exists theme.mv_railway_point cascade;
create materialized view theme.mv_railway_point as (
	select p.osm_id,railway,p.name,access,bridge,tunnel,layer,cutting,embankment,ref,service,usage
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and railway is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_railway_line cascade;
create materialized view theme.mv_railway_line as (
select p.osm_id,railway,p.name,access,bridge,tunnel,layer,cutting,embankment,ref,service,usage
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and railway is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_railway_polygone cascade;
create materialized view theme.mv_railway_polygone as (
	select p.osm_id,railway,p.name,access,bridge,tunnel,layer,cutting,embankment,ref,service,usage
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and railway is not null and a.code_insee = '93'
);

select logFile('B2-22 : Vues matérialisées route');
------------------------------------------------------
--route
drop materialized view if exists theme.mv_route_point cascade;
create materialized view theme.mv_route_point as (
	select p.osm_id,route,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and route is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_route_line cascade;
create materialized view theme.mv_route_line as (
	select p.osm_id,route,p.name,access,bridge,tunnel,layer,cutting,embankment,ref
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and route is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_route_polygone cascade;
create materialized view theme.mv_route_polygone as (
	select p.osm_id,route,p.name,access,bridge,tunnel,layer,cutting,embankment,ref
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and route is not null and a.code_insee = '93'
);

select logFile('B2-23 : Vues matérialisées shop');
------------------------------------------------------
--shop
drop materialized view if exists theme.mv_shop_point cascade;
create materialized view theme.mv_shop_point as (
	select p.osm_id,shop,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'boutique' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and shop is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_shop_line cascade;
create materialized view theme.mv_shop_line as (
	select p.osm_id,shop,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'boutique' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and shop is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_shop_polygone cascade;
create materialized view theme.mv_shop_polygone as (
	select p.osm_id,shop,p.name,access
		,true as activite_economique
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'boutique' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and shop is not null and a.code_insee = '93'
);

select logFile('B2-24 : Vues matérialisées sport');
------------------------------------------------------
--sport
drop materialized view if exists theme.mv_sport_point cascade;
create materialized view theme.mv_sport_point as (
	select p.osm_id,sport,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and sport is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_sport_line cascade;
create materialized view theme.mv_sport_line as (
	select p.osm_id,sport,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and sport is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_sport_polygone cascade;
create materialized view theme.mv_sport_polygone as (
	select p.osm_id,sport,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and sport is not null and a.code_insee = '93'
);

select logFile('B2-25 : Vues matérialisées telecom');
------------------------------------------------------
--telecom
drop materialized view if exists theme.mv_telecom_point cascade;
create materialized view theme.mv_telecom_point as (
	select p.osm_id,tags->'telecom' as telecom,p.name
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tags->'telecom' is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_telecom_line cascade;
create materialized view theme.mv_telecom_line as (
	select p.osm_id,tags->'telecom' as telecom,p.name
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tags->'telecom' is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_telecom_polygone cascade;
create materialized view theme.mv_telecom_polygone as (
	select p.osm_id,tags->'telecom' as telecom,p.name
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tags->'telecom' is not null and a.code_insee = '93'
);

select logFile('B2-26 : Vues matérialisées tourism');
------------------------------------------------------
--tourism
drop materialized view if exists theme.mv_tourism_point cascade;
create materialized view theme.mv_tourism_point as (
	select p.osm_id,tourism,p.name,access
		,(case when tourism in ('hotel','guest_house','camp_site')
			then true else false 
		end) as activite_economique	
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'tourisme' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tourism is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_tourism_line cascade;
create materialized view theme.mv_tourism_line as (
	select p.osm_id,tourism,p.name,access
		,(case when tourism in ('hotel','guest_house','camp_site')
			then true else false 
		end) as activite_economique	
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'tourisme' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tourism is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_tourism_polygone cascade;
create materialized view theme.mv_tourism_polygone as (
	select p.osm_id,tourism,p.name,access
		,(case when tourism in ('hotel','guest_house','camp_site')
			then true else false 
		end) as activite_economique	
		,(case when (tags::text like '%vacant%') then 'oui' else 'non' end) as local_vacant
		,(case when (tags::text like '%vacant%') then 'local vide' else 'tourisme' end) as type_activite
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and tourism is not null and a.code_insee = '93'
);

select logFile('B2-27 : Vues matérialisées water');
------------------------------------------------------
--water
drop materialized view if exists theme.mv_water_point cascade;
create materialized view theme.mv_water_point as (
	select p.osm_id,water,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and water is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_water_line cascade;
create materialized view theme.mv_water_line as (
	select p.osm_id,water,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and water is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_water_polygone cascade;
create materialized view theme.mv_water_polygone as (
	select p.osm_id,water,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and water is not null and a.code_insee = '93'
);

select logFile('B2-28 : Vues matérialisées waterway');
------------------------------------------------------
--waterway
drop materialized view if exists theme.mv_waterway_point cascade;
create materialized view theme.mv_waterway_point as (
	select p.osm_id,waterway,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,coordx,coordy,the_geom 
	from global.mv_habillage_osm_point as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and waterway is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_waterway_line cascade;
create materialized view theme.mv_waterway_line as (
	select p.osm_id,waterway,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,longueur,the_geom 
	from global.mv_habillage_osm_line as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and waterway is not null and a.code_insee = '93'
);
drop materialized view if exists theme.mv_waterway_polygone cascade;
create materialized view theme.mv_waterway_polygone as (
	select p.osm_id,waterway,p.name,access
		,osm_user,osm_timestamp,tags,date_import,num_keys,lastmodif,superficie,the_geom 
	from global.mv_habillage_osm_polygon as p, administratif.region_sud as a
	where ST_Intersects(a.geom, p.the_geom) and waterway is not null and a.code_insee = '93'
);

-------------------------------------------------------
select logFile('B3 : Création des tables spécifiques');
-------------------------------------------------------

select logFile('B3-01 : Vues matérialisées adresses');
------------------------------------------------------
--adresses
drop materialized view if exists theme.mv_adresses cascade;
create materialized view theme.mv_adresses as (
with ra as (
		select id as id_rel, primitive, id_membre, role, num_mem as nb_mem, type, nom
			, keys as keys_rel, num_keys as nb_keys_rel
		from global.mv_habillage_osm_membres_rel
		where type = 'associatedStreet' and primitive = 'n')
		, pa as (
		select osm_id,tags->'addr:housenumber' as addr_numbr,tags->'fixme' as fixme
			,tags->'addr:place' as ensemble,tags->'addr:flats' as porte,osm_user,osm_timestamp
			,tags,date_import,num_keys as nb_keys_obl,the_geom
		from theme.mv_region_point
		where tags->'addr:housenumber' is not null )
		, ca as ( 
		select id_com as id_com,nom_commune,geom
		from administratif.communes_2021)
	select * from pa 
	left join ra on pa.osm_id = ra.id_membre::bigint
	left join ca on st_within(pa.the_geom,ca.geom)
	order by type,addr_numbr
);

select logFile('B3-02 : Vues matérialisées voirie');
------------------------------------------------------
--voirie
drop materialized view if exists theme.mv_voirie cascade;
create materialized view theme.mv_voirie as (
	with v as (select osm_id,categorie,the_geom from theme.mv_highway_line)
	,r as (select *,keys->'ref:FR:commune' as id_interne 
			from global.mv_habillage_osm_membres_rel
			where type = 'associatedStreet' and primitive = 'w')
	select id,max(date_import) as date_import,nom,id_interne,min(categorie) as categorie, keys
		,st_unaryunion(unnest(ST_ClusterIntersecting(the_geom))) as the_geom 
	from r
		inner join v
			on r.id_membre::bigint = v.osm_id
	group by id, nom, id_interne, keys
	order by r.nom, id_interne
);

select logFile('B3-03 : Vues matérialisées référence des voies');
------------------------------------------------------------------
--référence des voies
--extraction de l'attribut "ref" dans les tags des relations (colonne "keys")
drop materialized view if exists analyses.voies_ref cascade;
create materialized view analyses.voies_ref as (
with ref_temp as (
	select max(date_import) as date_import, min(categorie) as categorie, keys->'ref' as ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from theme.mv_voirie
	where keys->'ref' is not null
	group by keys->'ref'
		union
	--extraction de l'attribut "ref" des tronçons
	select max(date_import) as date_import, min(categorie) as categorie, ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from theme.mv_highway_line
	where ref is not null
	group by ref
		union
	--extraction de l'attribut "loc_ref" dans les tags des relations (colonne "keys")
	select max(date_import) as date_import, min(categorie) as categorie, keys->'loc_ref' as ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from theme.mv_voirie
	where keys->'loc_ref' is not null
	group by keys->'loc_ref'
		union
	--extraction de l'attribut "loc_ref" des tronçons
	select max(date_import) as date_import, min(categorie) as categorie, loc_ref as ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from theme.mv_highway_line
	where loc_ref is not null
	group by loc_ref
		union
	--extraction de l'attribut "int_ref" dans les tags des relations (colonne "keys")
	select max(date_import) as date_import, min(categorie) as categorie, keys->'int_ref' as ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from theme.mv_voirie
	where keys->'int_ref' is not null
	group by keys->'int_ref'
		union
	--extraction de l'attribut "int_ref" des tronçons
	select max(date_import) as date_import, min(categorie) as categorie, int_ref as ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from theme.mv_highway_line
	where int_ref is not null
	group by int_ref	
)
--fusion des objets dans la table définitive
	select max(date_import) as date_import, min(categorie) as categorie, ref, st_unaryunion(unnest(st_clusterintersecting(the_geom))) as the_geom
	from ref_temp
	group by ref
);

select logFile('B3-04 : Vues matérialisées géodésie');
------------------------------------------------------
--géodésie
drop materialized view if exists theme.mv_geodesie cascade;
create materialized view theme.mv_geodesie as (
	select osm_id,man_made,name,tags->'ref' AS ref,tags->'ele' AS elevation
		,tags->'description' AS description,tags->'url' AS url,tags->'network' AS network
		,osm_user,osm_timestamp,tags,date_import,coordx,coordy,lastmodif,num_keys
		,the_geom
	from theme.mv_man_made_point where man_made = 'survey_point'
);

select logFile('B3-05 : Vues matérialisées activités économiques');
--------------------------------------------------------------------
--activités économiques
--activités économiques à partir de la table region_point
drop materialized view if exists theme.mv_activite_economique cascade;
create materialized view theme.mv_activite_economique as (
-- amenity : points	
	select osm_id,'amenity' as cle_semantique,amenity as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_amenity_point where activite_economique is true
		union
-- craft : points
	select osm_id,'craft' as cle_semantique,craft as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_craft_point where activite_economique is true	
		union
-- office : points
	select osm_id,'office' as cle_semantique,office as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_office_point where activite_economique is true
		union
-- shop : points
	select osm_id,'shop' as cle_semantique,shop as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_shop_point where activite_economique is true
		union
-- landuse : points
	select osm_id,'landuse' as cle_semantique,landuse as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_landuse_point where activite_economique is true
		union
-- tourism : points
	select osm_id,'tourism' as cle_semantique,tourism as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_tourism_point where activite_economique is true
		union 
-- amenity : polygones	
	select osm_id,'amenity' as cle_semantique,amenity as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'way' as primitive,osm_user,osm_timestamp,date_import,lastmodif
		,st_x(st_centroid(the_geom)) as coordx,st_y(st_centroid(the_geom)) as coordy
		,tags,st_centroid(the_geom) as the_geom
	from theme.mv_amenity_polygone where activite_economique is true
		union
-- craft : polygones
	select osm_id,'craft' as cle_semantique,craft as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'way' as primitive,osm_user,osm_timestamp,date_import,lastmodif
		,st_x(st_centroid(the_geom)) as coordx,st_y(st_centroid(the_geom)) as coordy
		,tags,st_centroid(the_geom) as the_geom
	from theme.mv_craft_polygone where activite_economique is true	
		union
-- office : polygones
	select osm_id,'office' as cle_semantique,office as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'way' as primitive,osm_user,osm_timestamp,date_import,lastmodif
		,st_x(st_centroid(the_geom)) as coordx,st_y(st_centroid(the_geom)) as coordy
		,tags,st_centroid(the_geom) as the_geom
	from theme.mv_office_polygone where activite_economique is true
		union
-- shop : polygones
	select osm_id,'shop' as cle_semantique,shop as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'way' as primitive,osm_user,osm_timestamp,date_import,lastmodif
		,st_x(st_centroid(the_geom)) as coordx,st_y(st_centroid(the_geom)) as coordy
		,tags,st_centroid(the_geom) as the_geom
	from theme.mv_shop_polygone where activite_economique is true
		union
-- landuse : polygones
	select osm_id,'landuse' as cle_semantique,landuse as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'way' as primitive,osm_user,osm_timestamp,date_import,lastmodif
		,st_x(st_centroid(the_geom)) as coordx,st_y(st_centroid(the_geom)) as coordy
		,tags,st_centroid(the_geom) as the_geom
	from theme.mv_landuse_polygone where activite_economique is true
		union
-- tourism : polygones
	select osm_id,'tourism' as cle_semantique,tourism as valeur_cle
		,name,tags->'ref:FR:SIRET' as siret,activite_economique,local_vacant,type_activite
		,'way' as primitive,osm_user,osm_timestamp,date_import,lastmodif
		,st_x(st_centroid(the_geom)) as coordx,st_y(st_centroid(the_geom)) as coordy
		,tags,st_centroid(the_geom) as the_geom
	from theme.mv_tourism_polygone where activite_economique is true
);

select logFile('B3-06 : Vues matérialisées éducation');
--------------------------------------------------------
--education : points
drop materialized view if exists theme.mv_education_point cascade;
create materialized view theme.mv_education_point as (
	select 
		osm_id,amenity,name,tags->'ref:UIA' as uia,tags->'ref:AEFE' as aefe
		,tags->'isced:level' as niveau_education,tags->'school:FR' as type_etablissement
		,tags->'ref' as reference,tags->'operator:type' as gestion
		,tags->'operator' as operateur,tags->'religion' as religion,tags->'source' as source
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,coordx,coordy
		,tags,the_geom
	from theme.mv_amenity_point 
	where amenity in ('kindergarten','school','university','college')
);

--education : polygones
drop materialized view if exists theme.mv_education_polygone cascade;
create materialized view theme.mv_education_polygone as (
	select 
		osm_id,amenity,name,tags->'ref:UIA' as uia,tags->'ref:AEFE' as aefe
		,tags->'isced:level' as niveau_education,tags->'school:FR' as type_etablissement
		,tags->'ref' as reference,tags->'operator:type' as gestion
		,tags->'operator' as operateur,tags->'religion' as religion,tags->'source' as source
		,'node' as primitive,osm_user,osm_timestamp,date_import,lastmodif,superficie
		,tags,the_geom
	from theme.mv_amenity_polygone 
	where amenity in ('kindergarten','school','university','college')
);

select logFile('B3-07 : Vues matérialisées vélotourisme');
-----------------------------------------------------------
--vélotorisme : tronçons des circuits
drop materialized view if exists theme.mv_velotourisme_troncon_lin cascade;
create materialized view theme.mv_velotourisme_troncon_lin as (
	select
		mr.id, mr.nom, coalesce(mr.keys -> 'rcn_ref',mr.keys -> 'ref') as ref, 
		mr.keys -> 'route' as route, 
		mr.keys -> 'distance' as distance, mr.keys -> 'duration' as duree, mr.keys -> 'colour' as couleur, 
		'<a href=https://trouver.datasud.fr/dataset/velotourisme-boucles-locales-region target="blank">Accéder à la plateforme</a>' as lien_datasud,
		hl.osm_id, mr.keys, hl.the_geom
	-- Réalisation d'une jointure entre la table public.habillage_osm_line et la table public.habillage_osm_membres_rel
	from global.mv_habillage_osm_membres_rel mr
	join global.mv_habillage_osm_line hl on id_membre = osm_id::text
	where mr.type = 'route' and mr.keys -> 'route' = 'bicycle'
);

--vélotorisme : circuits complets
drop materialized view if exists theme.mv_velotourisme_circuit_lin cascade;
create materialized view theme.mv_velotourisme_circuit_lin as (
	select osm_id, route, name, ref, tags -> 'network' as reseau
		, tags -> 'distance' as distance, tags -> 'duration' as duree, tags -> 'colour' as couleur, r.code_insee
	--création de l'attribut 'region' avec une valeur 'oui' ou 'non' lorsqu'un tronçon passe par un point de la region
		,(case when (st_intersects(hl.the_geom, r.geom) is true) then 'oui' else 'non' end) as region
		, tags, the_geom --, *
	from global.mv_habillage_osm_line hl, administratif.region_sud r
	where route = 'bicycle' and osm_id < 0 and st_intersects(hl.the_geom, r.geom) and hl.name is not null and r.code_insee = '93'
	order by osm_id desc
 );

select logFile('B3-08 : Vues matérialisées aéroports (Sud Foncier Eco)');
--------------------------------------------------------------------------
--aéroports (points)
drop materialized view if exists sfe.aeroports_pnt cascade;
create materialized view sfe.aeroports_pnt as (
select osm_id ,aeroway, name, tags->'iata' as iata, st_centroid(the_geom) as the_geom-- ,* 
from theme.mv_region_polygon mrp2 
where aeroway is not null and tags->'iata' is not null
);

--aéroports (polygones)
drop materialized view if exists sfe.aeroports_polygone cascade;
create materialized view sfe.aeroports_polygone as (
select osm_id ,aeroway, name, tags->'iata' as iata, the_geom-- ,* 
from theme.mv_region_polygon mrp2 
where aeroway is not null and tags->'iata' is not null
);

select logFile('B3-09 : Vues matérialisées arrêts transport (Sud Foncier Eco)');
---------------------------------------------------------------------------------
--arrêts transport (points)
drop materialized view if exists sfe.arret_transport_point cascade;
create materialized view sfe.arret_transport_point as (
select osm_id ,name, highway, amenity, railway, public_transport
	, tags->'bus' as bus, tags->'train' as train, tags->'subway' as subway, tags->'tram' as tram
	, tags->'ferry' as ferry, tags->'aerialway' as aerialway, tags->'trolleybus' as trolleybus
	, the_geom-- ,* 
from theme.mv_region_point 
where railway similar to '%(halt|station|tram_stop)%'
	or public_transport similar to '%(station|stop_position)%'
	or amenity similar to '%(bus_station|ferry_terminal|taxi)%'	
);

--arrêts transport (polygones)
drop materialized view if exists sfe.arret_transport_polygone cascade;
create materialized view sfe.arret_transport_polygone as (
select osm_id ,name, highway, amenity, railway, public_transport
	, tags->'bus' as bus, tags->'train' as train, tags->'subway' as subway, tags->'tram' as tram
	, tags->'ferry' as ferry, tags->'aerialway' as aerialway, tags->'trolleybus' as trolleybus
	, the_geom-- ,* 
from theme.mv_region_polygon
where railway similar to '%(halt|station|tram_stop)%'
	or public_transport like '%station%'
	or amenity similar to '%(bus_station|ferry_terminal|taxi)%'	
);

--arrêts transport (lignes)
drop materialized view if exists sfe.arret_transport_line cascade;
create materialized view sfe.arret_transport_line as (
select osm_id ,name, highway, amenity, railway, public_transport
	, tags->'bus' as bus, tags->'train' as train, tags->'subway' as subway, tags->'tram' as tram
	, tags->'ferry' as ferry, tags->'aerialway' as aerialway, tags->'trolleybus' as trolleybus
	, the_geom-- ,* 
from theme.mv_region_line 
where railway similar to '%(halt|station|tram_stop)%'
	or public_transport like '%station%'
	or amenity similar to '%(bus_station|ferry_terminal|taxi)%'	
);

select logFile('B3-10 : Vues matérialisées banques (Sud Foncier Eco)');
-------------------------------------------------------------------------
--banques (points)
drop materialized view if exists sfe.banque_points cascade;
create materialized view sfe.banque_points as (
select osm_id, name,cle_semantique , valeur_cle , the_geom-- ,* 
from theme.mv_activite_economique mae
where valeur_cle = 'bank'
);

select logFile('B3-11 : Vues matérialisées docteurs (Sud Foncier Eco)');
-------------------------------------------------------------------------
--santé (points)
drop materialized view if exists sfe.medecin_points cascade;
create materialized view sfe.medecin_points as (
select osm_id, name,cle_semantique , valeur_cle , tags->'healthcare' as healthcare, the_geom-- ,* 
from theme.mv_activite_economique mae
where valeur_cle = 'doctors'
	or tags->'healthcare' in ('centre','nurse')
);

select logFile('B3-12 : Vues matérialisées parking (Sud Foncier Eco)');
-------------------------------------------------------------------------
--parking (polygones to points)
drop materialized view if exists sfe.parking_points cascade;
create materialized view sfe.parking_points as (
select osm_id, name, amenity , tags->'hgv' as hgv, st_centroid(the_geom) as the_geom --,*
from theme.mv_amenity_polygone
where amenity = 'parking' and tags->'hgv' is not null and tags->'hgv' != 'no'
);

--parking (polygones)
drop materialized view if exists sfe.parking_polygone cascade;
create materialized view sfe.parking_polygone as (
select osm_id, name, amenity , tags->'hgv' as hgv, the_geom --,*
from theme.mv_amenity_polygone
where amenity = 'parking' and tags->'hgv' is not null and tags->'hgv' != 'no'
);

select logFile('B3-13 : Vues matérialisées hôtels (Sud Foncier Eco)');
-----------------------------------------------------------------------
--hotels (points and polygones to points)
drop materialized view if exists sfe.hotels_point cascade;
create materialized view sfe.hotels_point as (
select osm_id, name, tourism, the_geom --, * 
from theme.mv_tourism_point
where tourism = 'hotel'
	union
select osm_id, name, tourism, st_centroid(the_geom) as the_geom
from theme.mv_tourism_polygone
where tourism = 'hotel'
);

--hotels (polygones)
drop materialized view if exists sfe.hotels_polygone cascade;
create materialized view sfe.hotels_polygone as (
select osm_id, name, tourism, the_geom
from theme.mv_tourism_polygone
where tourism = 'hotel'
);

select logFile('B3-14 : Vues matérialisées ports (Sud Foncier Eco)');
----------------------------------------------------------------------
--ports (polygones to points)
drop materialized view if exists sfe.ports_points cascade;
create materialized view sfe.ports_points as (
select osm_id, name, tags->'industrial' as industrial, st_centroid(the_geom) as the_geom
from theme.mv_landuse_polygone mlp 
where landuse = 'port' or tags->'industrial' = 'port'
);

--ports (polygones)
drop materialized view if exists sfe.ports_polygones cascade;
create materialized view sfe.ports_polygones as (
select osm_id, name, tags->'industrial' as industrial, the_geom
from theme.mv_landuse_polygone mlp 
where landuse = 'port' or tags->'industrial' = 'port'
);

select logFile('B3-15 : Vues matérialisées poste (Sud Foncier Eco)');
----------------------------------------------------------------------
--poste (points and polygones to points)
drop materialized view if exists sfe.poste_points cascade;
create materialized view sfe.poste_points as (
select osm_id, name, amenity, the_geom
from theme.mv_amenity_point mlp
where amenity = 'post_office'
	union
select osm_id, name, amenity, st_centroid(the_geom) as the_geom 
from theme.mv_amenity_polygone map2 
where amenity = 'post_office'
);

--poste (polygones)
drop materialized view if exists sfe.poste_polygone cascade;
create materialized view sfe.poste_polygone as (
select osm_id, name, amenity, the_geom --, * 
from theme.mv_amenity_polygone map2 
where amenity = 'post_office'
);

select logFile('B3-16 : Vues matérialisées tramway (Sud Foncier Eco)');
------------------------------------------------------------------------
--tramway (lignes)
drop materialized view if exists sfe.tramway_line cascade;
create materialized view sfe.tramway_line as (
select osm_id, name, railway, the_geom --, * 
from theme.mv_railway_line mal 
where railway = 'tram'
);

select logFile('B3-17 : Vues matérialisées autoroutes (Sud Foncier Eco)');
----------------------------------------------------------------------------
--autoroutes (lignes)
drop materialized view if exists sfe.voies_principales_line cascade;
create materialized view sfe.voies_principales_line as (
select osm_id, name, highway, the_geom --, * 
from theme.mv_highway_line mal 
--where highway in ('motorway','trunk','primary')
where highway similar to '%(motorway|trunk|primary)%'
);

select logFile('B3-18 : Vues matérialisées acces_autoroutes (Sud Foncier Eco)');
----------------------------------------------------------------------------------
--poste (points)
drop materialized view if exists sfe.acces_autoroutes_points cascade;
create materialized view sfe.acces_autoroutes_points as (
select osm_id, name, highway, '' as barrier, the_geom --, * 
from theme.mv_highway_point mlp 
where highway = 'motorway_junction'
union
select osm_id, name, '' as highway, barrier, the_geom --, *
from theme.mv_barrier_point mbp 
where barrier ='toll_booth'
);

select logFile('B3-19 : Vues matérialisées restaurants (Sud Foncier Eco)');
----------------------------------------------------------------------
--poste (points and polygones to points)
drop materialized view if exists sfe.restaurants_points cascade;
create materialized view sfe.restaurants_points as (
select osm_id, name, amenity, the_geom
from theme.mv_amenity_point mlp
where amenity = 'restaurant'
	union
select osm_id, name, amenity, st_centroid(the_geom) as the_geom 
from theme.mv_amenity_polygone map2 
where amenity = 'restaurant'
);

--poste (polygones)
drop materialized view if exists sfe.poste_polygone cascade;
create materialized view sfe.poste_polygone as (
select osm_id, name, amenity, the_geom --, * 
from theme.mv_amenity_polygone map2 
where amenity = 'post_office'
);

select logFile('B3-20 : Vues matérialisées stations de ski';
----------------------------------------------------------------------

--https://wiki.openstreetmap.org/wiki/FR:Piste_Maps


-- Remontées mécaniques : aerialway (points)  ** OK **
drop materialized view if exists theme.mv_remontees_mecaniques_pnt cascade;
create materialized view theme.mv_remontees_mecaniques_pnt as (
select name,tags->'aerialway' as aerialway, 
	tags->'aerialway:occupancy' as occupancy, tags->'aerialway:capacity' as capacity, tags->'aerialway:duration' as duration, tags->'aerialway:bubble' as bubble, tags->'aerialway:heating' as heating, 
	tags, the_geom
from theme.mv_region_point
where tags->'aerialway' is not null
);

-- Remontées mécaniques : aerialway (lignes)  ** OK **
drop materialized view if exists theme.mv_remontees_mecaniques_lin cascade;
create materialized view theme.mv_remontees_mecaniques_lin as (
select name,tags->'aerialway' as aerialway, 
	tags->'aerialway:occupancy' as occupancy, tags->'aerialway:capacity' as capacity, tags->'aerialway:duration' as duration, tags->'aerialway:bubble' as bubble, tags->'aerialway:heating' as heating, 
	tags, the_geom
from theme.mv_region_line mrp
where tags->'aerialway' is not null
);

-- Pistes : piste:type (lignes)  ** OK **
drop materialized view if exists theme.mv_piste_ski_lin cascade;
create materialized view theme.mv_piste_ski_lin as (
select name,tags->'piste:type' as type_piste, man_made, tags->'surface' as surface,
	tags->'piste:difficulty' as difficulty, tags->'aerialway' as aerialway, tags->'piste:grooming' as grooming, tags->'site' as site
	tags, the_geom
from theme.mv_region_line mrp
where tags->'piste:type' is not null
);

-- Pistes : piste:type (polygones)  ** OK **
drop materialized view if exists theme.mv_piste_ski_pol cascade;
create materialized view theme.mv_piste_ski_pol as (
select name,tags->'piste:type' as type_piste, area, man_made, tags->'surface' as surface,
	tags->'piste:difficulty' as difficulty, tags->'aerialway' as aerialway, tags->'piste:grooming' as grooming, tags->'site' as site
	tags, the_geom
from theme.mv_region_polygon
where tags->'piste:type' is not null
);

--station de ski : landuse=winter_sport (polygones)  ** OK **
drop materialized view if exists theme.mv_stations_ski_pol cascade;
create materialized view theme.mv_stations_ski_pol as (
select name,landuse, sport, tags, the_geom
from theme.mv_region_polygon mrp
where landuse='winter_sports' or (landuse='recreation_ground' and sport='skiing')
);


--------------------------------------------------------
select logFile('B4 : Création des tables statistiques');
--------------------------------------------------------

select logFile('B4-01 : Vues matérialisées liste des utilisateurs');
--------------------------------------------------------------------
--users_list
drop materialized view if exists analyses.users_list;
create materialized view analyses.users_list as (
	with
		point_users_list as(
			select osm_user, max(date_import) as date_import, count(osm_user) as nb_point, max(osm_timestamp) as last_modif  
				from theme.mv_region_point 
				group by osm_user),
		line_users_list as(
			select osm_user, max(date_import) as date_import, count(osm_user) as nb_line, max(osm_timestamp) as last_modif  
				from theme.mv_region_line 
				group by osm_user),
		polygon_users_list as(
			select osm_user, max(date_import) as date_import, count(osm_user) as nb_polygon, max(osm_timestamp) as last_modif  
				from theme.mv_region_polygon 
				group by osm_user)
	select osm_user, 
		sum(coalesce(point_users_list.nb_point,0)) as nb_point, 
		sum(coalesce(line_users_list.nb_line,0)) as nb_line, 
		sum(coalesce(polygon_users_list.nb_polygon,0)) as nb_polygon, 
		sum(coalesce(point_users_list.nb_point,0) + coalesce(line_users_list.nb_line,0) + coalesce(polygon_users_list.nb_polygon,0)) as nb_object,
		max(date_import) as date_import,
		max(last_modif) as last_modif 
		from point_users_list
   		natural full join line_users_list
   		natural full join polygon_users_list
   		group by osm_user
   		order by nb_object desc);

select logFile('B4-02 : Vues matérialisées liste des clés');
--------------------------------------------------------------------
--tags_list
drop materialized view if exists analyses.tags_list;
create materialized view analyses.tags_list as (
	with
		point_tags_list as(
			select key, max(date_import) as date_import, count(key) as nb_point, max(osm_timestamp) as last_modif 
				from theme.mv_region_point, skeys(tags) key 
				group by key),
		line_tags_list as(
			select key, max(date_import) as date_import, count(key) as nb_line, max(osm_timestamp) as last_modif 
				from theme.mv_region_line, skeys(tags) key 
				group by key),
		polygon_tags_list as(
			select key, max(date_import) as date_import, count(key) as nb_polygon, max(osm_timestamp) as last_modif 
				from theme.mv_region_polygon, skeys(tags) key 
				group by key)
	select key, 
		sum(coalesce(point_tags_list.nb_point,0)) as nb_point, 
		sum(coalesce(line_tags_list.nb_line,0)) as nb_line, 
		sum(coalesce(polygon_tags_list.nb_polygon,0)) as nb_polygon, 
		sum(coalesce(point_tags_list.nb_point,0) + coalesce(line_tags_list.nb_line,0) + coalesce(polygon_tags_list.nb_polygon,0)) as nb_object,
		max(date_import) as date_import,
		max(last_modif) as last_modif
		from point_tags_list
   		natural full join line_tags_list
   		natural full join polygon_tags_list
   		group by key
   		order by nb_object desc);

select logFile('B4-03 : Autorisations des accés aux tables');
--------------------------------------------------------------------
--autorisation d'accés aux tables
grant all on all tables in schema administratif to administrateur;
grant all on all tables in schema analyses to administrateur;
grant all on all tables in schema global to administrateur;
grant all on all tables in schema public to administrateur;
grant all on all tables in schema sfe to administrateur;
grant all on all tables in schema theme to administrateur;

grant select on all tables in schema administratif to utilisateur;
grant select on all tables in schema analyses to utilisateur;
grant select on all tables in schema global to utilisateur;
grant select on all tables in schema public to utilisateur;
grant all on all tables in schema sfe to utilisateur;
grant select on all tables in schema theme to utilisateur;
------------------------------------------------------------
select logFile('B5 : FIN DES TRAITEMENTS POUR LA PARTIE B');
------------------------------------------------------------

do $$
	declare
		mt record;
		mtname text;
		mtindexname text;
	begin
		for mt in 
		select * 
		from pg_matviews
		where schemaname = 'theme' and hasindexes is false
		loop
			--if select osm_id, count(*) from theme.mv_velotourisme_troncon_lin group by osm_id having count(*) > 1
			raise info '1- Vue testée : %',mt;
			mtname = mt.matviewname;
			raise info '2- Nom de la vue : %',mtname;
			mtindexname = 'index_'||mtname;
			raise info '3- Nom de l''index de la vue : %',mtindexname;
			execute format ('create unique index '||mtindexname||' on theme.'||mtname||' (osm_id);');
			--exception when unique_violation then end;
		end loop;
	end
$$;

