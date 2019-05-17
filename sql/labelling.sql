set role el_salvador_mined_education_write;

drop table if exists staging.labels;
create table if not exists staging.labels as
    with distinct_id_grado as (
        select distinct students.student, events.grado_code, events.year_range
        from staging.students as students
        left join staging.events as events
        on students.student = events.student )
    select student, year_range, grado_code,
        case
        when grado_code>(11 - (label_window)) or lower(year_range) > ('2017-01-01'::date - interval '(label_window) year')::date
        then null::bool
        when student not in ( select student from distinct_id_grado as b where lower(b.year_range) = (lower(a.year_range) + interval '(label_window) year')::date )
        then true
        else false
        end as label_(label_window)y
        from distinct_id_grado as a;