set role el_salvador_mined_education_write;


-- select students that where in PRIMERO GRADO in 2009
select count(distinct student)
from staging.events
where extract(year from lower(year_range)) = '2009'
and grado_code = 1;


-- count how many students dropout after PRIMERO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2009'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1))
select label, count(*)
from list_students
group by label;

-- count how many students dropout after SEGUNDO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2010'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after TERCERO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2011'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after CUARTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2012'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after QUINTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2013'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after SEXTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2014'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after SEPTIMO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2015'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students starts PRIMERO BACHILLERATO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2018'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student)
from list_students;

-- count how many students starts SEGUNDO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2010'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;


-- count how many students starts TERCERO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2011'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students starts CUARTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2012'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students starts QUINTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2013'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students starts SEXTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2014'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students starts SEPTIMO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2015'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students starts OCTAVO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2016'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students starts NOVENO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2017'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;


-- count how many students starts BACHILLERATO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2018'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;set role el_salvador_mined_education_write;

-- select students that where in PRIMERO GRADO in 2009
select count(distinct student)
from staging.events
where extract(year from lower(year_range)) = '2009'
and grado_code = 1;


-- count how many students dropout after PRIMERO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2009'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1))
select label, count(*)
from list_students
group by label;

-- count how many students dropout after SEGUNDO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2010'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after TERCERO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2011'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after CUARTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2012'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after QUINTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2013'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after SEXTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2014'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students dropout after SEPTIMO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2015'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select label, count(*)
from list_students
group by label;

-- count how many students start PRIMERO BACHILLERATO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2018'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student)
from list_students;

-- count how many students start SEGUNDO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2010'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;


-- count how many students start TERCERO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2011'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students start CUARTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2012'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students start QUINTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2013'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students start SEXTO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2014'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students start SEPTIMO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2015'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students start OCTAVO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2016'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;

-- count how many students start NOVENO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2017'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;


-- count how many students start BACHILLERATO
with list_students as (
	select student, label
	from staging.labels
	where extract(year from lower(year_range)) = '2018'
	and student in (select distinct student from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)
)
select count(distinct student) as total_students, count(distinct student)/(select count(distinct student) from staging.events where extract(year from lower(year_range)) = '2009' and grado_code = 1)::float as prop
from list_students;
