set role el_salvador_mined_education_write;

-- compute our % of dropout r ates
with total_dropouts(
	select count(*) as total_dropouts, year_range
	from staging.labels 
	group by year_range
	having label = 1
),
total_students(
	select count(students) as total_students, year_range
	from staging.labels
	group by year_range
)
select extract(year from lower(a.year_range)), a.total_dropouts, b.total_students
from total_dropouts a inner join total_students b
on a.year_range = b.year_range;


-- compute partner's % dropout rates
with totals_matricula_final as(
	-- this with extracts a table with the total students at the end of the year (matricula_final) per school and per year.
select school, substr(original_table, length(original_table)-3, 4) as year, sum(matricula_final::int) as matricula_final
from preproc."9_joined"
group by school, substr(original_table, length(original_table)-3, 4)
),
total_matricula_inicial as(
	-- this with extracts a table with the total registered students at the beginning of the year (matricula_final) per school and per year.
select school_id as school, extract(year from lower(year_range))::varchar as year, count(*) as matricula_inicial
from cleaned."2_cleaned"
group by school_id, year_range
),
-- this with extracts a table with the total students at the beginning of the year (matricula_inicial) at the end (matricula_final) and the differences, per school, per year.
total_diff as(
	select a.school, a.year, a.matricula_final, b.matricula_inicial, -(matricula_final- matricula_inicial) as total_dropouts from totals_matricula_final a inner join 
	total_matricula_inicial b on a.school = b.school
	and a.year = b.year
	where -(matricula_final- matricula_inicial) > 0
	-- select only schools with dropouts
),
totals as(
	select sum(total_dropouts) as total_dropouts, sum(matricula_inicial) as total_students, year
	from total_diff
	group by year
)
select year, total_dropouts, total_students, total_dropouts/total_students::float as rate_intrayear
from totals;

