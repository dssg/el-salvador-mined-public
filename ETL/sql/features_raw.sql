-- RUN TIME: 2.8min

-- Feature list: age, student_female, year_range, student, grado_code, family_members, commute, departamento, municipio, school, school_departamento, school_municipio, school_coord1, school_coord2, school_index, school_difference, school_mismatch, school_owner_own, school_owner_borrow, school_owner_lease, school_owner_loan, school_owner_other, school_native, school_electricitycompany, school_electricity, school_classrooms, school_teachers, school_public, school_rural, school_toilet, school_toiletworks, school_toiletpit, school_toiletpitworks, school_toiletseptic, school_toiletsepticworks, school_eight_interventions, school_eight_program_gendered_violence, school_eight_program_pase, school_eight_program_safe_school, school_eight_program_safe_school_count, school_eight_vaso_de_leche, school_eight_prostitution, school_eight_rape, school_eight_rape2,school_dropout_gang_presence,school_dropout_work,school_drugs,school_extorsion,school_sexual_exploitation,school_dropout_gang_violence,school_rape,school_dropout_sexual_violence,school_community_gangs,school_community_trafficking,school_dropout_pregnancy,school_dropout_migration,school_community_rape,school_community_blades,school_gangs,school_dropout_abuse,school_dropout_prostitution,school_community_theft

set role el_salvador_mined_education_write;

drop table if exists results.features_raw;
create table results.features_raw as (
    select 
    -- STUDENTS
        date_part('year', age(lower(events.year_range), students.birth_date)) + (date_part('month', age(lower(events.year_range), students.birth_date))/12) as age ,
        case students.gender when 'm' then 0 else 1 end as student_female,
    -- EVENTS
        events.year_range as year_range ,
        events.student as student,
        events.grado_code as grado_code,
        events.family_members as family_members,
        events.commute as commute,
        events.dpto_name as departamento,
        events.munic_name as municipio,
    -- SCHOOL
        events.school as school,
        -- Location
        school.departamento as school_departamento, school.municipio as school_municipio, 
        school.coord1 as school_coord1, school.coord2 as school_coord2,
        -- Security index
        school.new_index as school_index, 
        -- Reporting ability
        school.difference as school_difference, school.census_mismatch as school_mismatch, 
        -- Ownership. Essentially OneHotEncoding the school ownership information. Also standardises the information from multiple years, since some years store information in the 'owner' column, but other years have already OneHotEncoded this information into 'own', 'borrow', 'lease', 'loan' and 'own_other'. 
        case when owner is not null and true = any(select s ~ 'propiedad' from unnest(string_to_array(owner, ',')) s) 
            then true
            when owner is not null and false = any(select s ~ 'propiedad' from unnest(string_to_array(owner, ',')) s)
            then false
            else own end as school_owner_own,
        case when owner is not null and true = any(select s ~ 'prestada' from unnest(string_to_array(owner, ',')) s) 
            then true
            when owner is not null and false = any(select s ~ 'prestada' from unnest(string_to_array(owner, ',')) s)
            then false
            else borrow end as school_owner_borrow,
        case when owner is not null and true = any(select s ~ 'alquilada' from unnest(string_to_array(owner, ',')) s) 
            then true
            when owner is not null and false = any(select s ~ 'alquilada' from unnest(string_to_array(owner, ',')) s)
            then false
            else lease end as school_owner_lease,
        case when owner is not null and true = any(select s ~ 'comodato' from unnest(string_to_array(owner, ',')) s) 
            then true
            when owner is not null and false = any(select s ~ 'comodato' from unnest(string_to_array(owner, ',')) s)
            then false
            else loan end as school_owner_loan,
        case when owner is not null and true = any(select s ~ 'otro' from unnest(string_to_array(owner, ',')) s) 
            then true
            when owner is not null and false = any(select s ~ 'otro' from unnest(string_to_array(owner, ',')) s)
            then false
            else own_other end as school_owner_other,
        -- General 
        school.native_region as school_native,
        case school.rural 
        when 'rural' then true
        else false end as school_rural, 
        case school.public 
        when 'public' then true
        else false end as school_public, 
        school.num_teachers as school_teachers, school.classrooms as school_classrooms, school.electricity as school_electricity, school.electricity_company as school_electricitycompany, 
        -- Toilet information. This will have to be simplified, since some years have the 'toilet' column, but other years have 'toilet_works' and 'school_toilet_notworks', and similarly for other types of toilet
        coalesce(school.toilet, school.toilet_works + school.toilet_no_works) as school_toilet,
        school.toilet_works / NULLIF(coalesce(school.toilet, school.toilet_works + school.toilet_no_works), 0) as school_toiletworks,
        coalesce(school.toilet_pit, school.toilet_pit_works + school.toilet_pit_no_works) as school_toiletpit,
        school.toilet_pit_works / NULLIF(coalesce(school.toilet_pit, school.toilet_pit_works + school.toilet_pit_no_works), 0) as school_toiletpitworks,
        coalesce(school.toilet_septic, school.toilet_septic_works + school.toilet_septic_no_works) as school_toiletseptic,
        school.toilet_septic_works / NULLIF(coalesce(school.toilet_septic, school.toilet_septic_works + school.toilet_septic_no_works), 0) as school_toiletsepticworks,
        -- Information from folder 8
        case when school.interventions is not null then true else false end as school_eight_interventions, -- These are strings stating the interventions present at the school. Instead, we simplify the column to 'true' if they have interventions, or 'false' for no interventions listed.
        school.program_gendered_violence as school_eight_program_gendered_violence, 
        school.program_pase as school_eight_program_pase, school.program_safe_school as school_eight_program_safe_school,
        array_length(string_to_array(NULLIF(regexp_replace(school.program_safe_school, ' y ', ','), ''), ','), 1) as school_eight_program_safe_school_count, 
        school.vaso_de_leche as school_eight_vaso_de_leche, school.prostitution as school_eight_prostitution, 
        school.rape as school_eight_rape, school.rape2 as school_eight_rape2,
        school.ce_causas_desercion_presencia_pandillas as school_dropout_gang_presence, 
        school.ce_causas_desercion_incorporacion_trabajo as school_dropout_work,
        school.factores_ce_drogas as school_drugs,
        school.factores_ce_extorsiones as school_extorsion,
        school.factores_comunidad_explotacion_sexual as school_sexual_exploitation,
        school.ce_causas_desercion_violencia_pandilleril as school_dropout_gang_violence,
        school.factores_ce_violaciones as school_rape,
        school.ce_causas_desercion_violencia_sexual as school_dropout_sexual_violence,
        school.factores_comunidad_maras as school_community_gangs,
        school.factores_comunidad_trata as school_community_trafficking,
        school.ce_causas_desercion_embarazo as school_dropout_pregnancy,
        school.ce_causas_desercion_migracion as school_dropout_migration,
        school.factores_comunidad_violaciones as school_community_rape,
        school.factores_comunidad_armas_blancas as school_community_blades,
        school.factores_ce_maras as school_gangs,
        school.ce_causas_desercion_acoso as school_dropout_abuse,
        school.ce_causas_desercion_prostitucion as school_dropout_prostitution,
        school.factores_comunidad_robos as school_community_theft
    from staging.events as events
        left join staging.students as students on students.student = events.student
        left join staging.schools as school on events.year_range = school.year_range and events.school = school.school
);

create index raw_year_range_student_idx on results.features_raw(year_range, student);