set role el_salvador_mined_education_write;

drop table if exists results.features;
create table results.features as (
    select events.year_range as year_range,
    events.student as student,
    case
        when date_part('year', age(lower(events.year_range), students.birth_date)) is NULL
        then 0
        else date_part('year', age(lower(events.year_range), students.birth_date))
        end as age,
    coalesce(students.family_members[1], 0) as family_members,
    case students.gender when 'm' then 0 else 1 end as is_female
    from staging.students as students
    left join staging.events as events
    on events.student = students.student
);
