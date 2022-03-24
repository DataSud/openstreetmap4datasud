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