create table public.test as 
with grid as (
SELECT (ST_SquareGrid(0.1, ST_Union(ST_Buffer(geom::geography, 50000)::geometry))).geom AS geom
FROM funai.pa_br_terrasindigenas_funai_2024
) select * from grid;

drop table public.test;


create table public.ti_union as 
select st_union(st_buffer(geom::geography, 50000)::geometry) geom from funai.pa_br_terrasindigenas_funai_2024


create index ti_union_geom_idx on public.ti_union using gist (geom);

create index test_geom_idx on public.test using gist (geom);


create table public.grid_ti_insersection as 
select b.geom from public.ti_union a
inner join public.test b on ST_Touches(a.geom, b.geom)



drop table grid_ti_insersection