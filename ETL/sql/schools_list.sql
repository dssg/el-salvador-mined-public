-- query that extracts schools to intervent

with joining_schools as(
select a.experiment, a.model, a.student, a.score, b.year_range, b.school from results.predictions a left join staging.events b 
on a.student = b.student and substring(a.year, '[0-9]{4}')::int = extract(year from lower(b.year_range))
where experiment = %s and model = %s
order by a.score desc
),
row_num as(
select row_number() over w1 as rownum, score, student, year_range, school, experiment, model
from joining_schools
window w1 as (order by score desc)
),
row_num_perc as(
select experiment, model, rownum, score, student, year_range, school, 
case when rownum < 0.1*(select count(*) from row_num) then 1 else 0 end as student_at_10 from row_num
),
total_students as(
select experiment, model, year_range, school, avg(score) as avg_score, sum(student_at_10) as total_students_at_10, count(student) as total_students
from row_num_perc
group by school, experiment, model, year_range
)
select experiment, model, extract(year from lower(year_range)), school, avg_score, total_students_at_10, 100*total_students_at_10/total_students::float as perc_students_at_10
from total_students order by avg_score desc, total_students_at_10 desc nulls last;
