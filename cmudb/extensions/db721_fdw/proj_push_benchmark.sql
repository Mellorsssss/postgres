-- simple select
-- select * from db721_farm;
-- select count(*) from db721_farm;
-- select * from db721_chicken;
-- select count(*) from db721_chicken;

-- select with where clause
-- select * from db721_farm, db721_chicken where db721_farm.farm_name = 'Incubator';

-- projection push down
-- select * from db721_chicken;
-- select * from db721_chicken, db721_farm;
-- select identifier from db721_chicken;
select db721_chicken.identifier from db721_chicken, db721_farm;
-- select * from db721_chicken, db721_farm;
-- select sex from db721_chicken;

-- predicate push down
-- select * from db721_chicken where notes = 'WOODY';

-- (maybe) aggregate push down

-- complex query

select identifier from db721_chicken;
select farm_name from db721_farm;
select db721_chicken.identifier, db721_farm.farm_name from db721_chicken, db721_farm;
