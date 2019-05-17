et role el_salvador_mined_education_write;

-- COMPARING OUR LABEL (staging.events and staging.labels) WITH GRADUATION DATA (cleaned.12_cleaned)
-- Question I: All students in labels are in 12_cleaned and viceversa?

-- Question I: All students in labels are in 12_cleaned and viceversa?
-- total students in both datasets
select distinct count(student)
from staging.events
where grado_code > 9 
and extract(year from lower(year_range)) > 2008 and extract(year from lower(year_range)) < 2018;

select distinct count(student)
from cleaned."12_cleaned"
where extract(year from lower(year_range)) > 2008 and extract(year from lower(year_range)) < 2018;

select * from cleaned."12_cleaned";

select count(*)
from
(select student, grado_code, year_range, promotion_status 
	from cleaned."12_cleaned") as a
left join
(select id, grado_code, year_range 
	from cleaned."6_cleaned") as b
on a.student = b.id
where b.id is null;

-- In 12, 324 more students. Where are they (schools)?
select school_name, count(*)
from
(select student, grado_code, year_range, promotion_status, school_name 
	from cleaned."12_cleaned") as a
left join
(select id, grado_code, year_range
	from cleaned."6_cleaned") as b
on a.student = b.id
where b.id is null
group by school_name;

with schools_in_12_not_6 as(
	select distinct school
	from
	(select student, grado_code, year_range, promotion_status, school 
		from cleaned."12_cleaned") as a
	left join
	(select id, grado_code, year_range
		from cleaned."6_cleaned") as b
	on a.student = b.id
	where b.id is null
	group by school
)
select school from schools_in_12_not_6 where school not in (select distinct school from cleaned."6_cleaned");

select * from cleaned."12_cleaned";

select count(*)
from
(select id, grado_code, year_range
	from cleaned."6_cleaned") as a
left join
(select student, grado_code, year_range, promotion_status
	from cleaned."12_cleaned") as b
on a.id = b.student
where a.id is null;

-- Assumption I: All students with label = 0 should be promotion_status = passed
select label, promotion_status, count(*)
from staging.labels left join cleaned."12_cleaned" on labels.student = "12_cleaned".student and labels.year_range = "12_cleaned".year_range
group by label, promotion_status;
-- Those students that are NULLS & failed should be a 1.

-- Are duplicates in 12?
select student, year, count(*)
from cleaned."12_cleaned"
group by student, year
having count(*) > 1;
-- YES!
