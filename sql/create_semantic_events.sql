set role el_salvador_mined_education_write;

drop table if exists staging.events;

create table staging.events as 
select a.address, a.bach_especialidad, a.bach_modalidad, a.bach_opcion, a.class_group, a.dpto, a.dpto_name, a.family_members, a.grado, a.grado_code, a.id as student, a.illness, a.medio_transporte as commute, a.munic, a.munic_name, a.school, a.status, a.year_range, b.promotion_status, now() as created_at
from 
cleaned."6_cleaned" as a
left join
cleaned."12_cleaned" as b
on a.id = b.student and a.year_range = b.year_range and a.school = b.school and a.grado_code = b.grado_code; -- This left join is giving more students than were in 6_cleaned
