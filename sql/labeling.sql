-- Run time: 20min

set role el_salvador_mined_education_write;

drop table if exists staging.labels;

create table if not exists staging.labels as
    with partitioned as (  -- First select an array partitioned by student, year
	select student, year_range, 
		array_agg(distinct grado_code) as grado_codes, -- Aggregates of grado_codes and promotion_status within the year
		array_agg(distinct promotion_status) as passed,
		case
	       	    when 'GENERAL' = any(array_agg(bach_modalidad)) then 11  -- If the student is in any bachillerato general during the year, the final year is considered to be 11. This fails for students that are in General in 11, move to Vocacional in 11, and then dropout.
		    when ARRAY['TECNICO VOCACIONAL', 'APREMAT', 'PILET'] && array_agg(bach_modalidad) then 12  -- If the student is in any of the other types of bachillerato, the final year is 12.
		    else NULL::int end as final_bach_year,	
		lead(year_range) over (partition by student order by year_range) as next_year
    from staging.events 
    group by student, year_range)
    select student, year_range, case 
    when lower(year_range) > ('2017-01-01'::date - interval '1 year')::date  -- TODO: generalise this to take parameters from the config file
	then null
    when next_year is not null  -- If a student is present the next year, they did not drop out
	then 0
    when final_bach_year <= any(grado_codes) and 'passed' = any(passed)  -- If they reached the final year of bachillerato and they passed, they did not drop out
	then 0
-- We consider students that disappear after noveno to have dropped out. 
-- Uncomment the following two lines if you do not want to make that assumption.
--    when 9 = any(grado_codes)
--	then null
    else 1 end as label
    from partitioned;
    
create index labels_year_range_student_idx on staging.labels(year_range, student);