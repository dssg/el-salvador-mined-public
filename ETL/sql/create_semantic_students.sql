set role el_salvador_mined_education_write;

-- drop old table
drop table if exists staging.students;

-- create staging.students 
-- takes ~300s
create table staging.students
as
select distinct on(id)
id as student, gender,
(array_agg(birth_date order by year_range asc))[1]::date as birth_date,
-- this row select the first birth_date that appears in the database (ordered by year_range) 
array_agg(distinct birth_date) as birth_dates, 
array_agg(distinct responsable) as responsables, 
array_agg(distinct mother) as mothers, 
array_agg(distinct father) as fathers, 
now() as created_at
from cleaned."6_cleaned"
group by id, gender
order by id, birth_date desc;
