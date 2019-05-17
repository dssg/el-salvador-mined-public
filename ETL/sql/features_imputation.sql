-- Feature list: year_range,student,age,student_female,grado_code,family_members,commute,departamento,municipio,school_departamento,school_municipio,school_difference,school_mismatch,school_teachers,school_classrooms,school_toilet,school_native,school_rural,school_public,overage1,overage2,overage_bool,overage2_bool,repeats,repeats_bool
-- RUN TIME: 40min
-- Here we are arbitrarily choosing to simplify the aggregate features coming from reducing the data to a single row per student per year. For boolean variables, we've choosen for them to be true if any are true. For numeric variables, we've chosen the average. There is commented code that can generalise this if needed.

set role el_salvador_mined_education_write;

drop table if exists results.features_imputed;
create table results.features_imputed as (
    -- Feature imputation using mode(), and grouping by year_range and grado_code
    with group_by_year_grado as (
        select
            year_range, round(grado_code_avg) as grado_code,
            avg(age_avg) as age_avg_avg,
            mode() within group (order by student_female) as student_female_mode,
            NULLIF(mode() within group (order by commute),'') as commute_mode
        from results.features_aggregate
        group by year_range, round(grado_code_avg)
    ),
    -- Feature imputation using mode() and grouping by school. This should *only* be used for variables that are constant in time for schools, since it uses data from the future to impute past data.
    group_by_school as (
        select school,
            coalesce(NULLIF(mode() within group (order by school_departamento), ''),NULLIF(mode() within group (order by departamento), '')) as school_departamento_mode,
            coalesce(NULLIF(mode() within group (order by school_municipio), ''),NULLIF(mode() within group (order by municipio), '')) as school_municipio_mode,
            mode() within group (order by school_native_any) as school_native_any_mode,
/*             mode() within group (order by school_native_all) as school_native_all_mode, */
            mode() within group (order by school_rural_any) as school_rural_any_mode,
/*             mode() within group (order by school_rural_all) as school_rural_all_mode, */
            mode() within group (order by school_public_any) as school_public_any_mode
/*             mode() within group (order by school_public_all) as school_public_all_mode */
        from results.features_aggregate
        group by school
    ),
    group_by_region as (
        select year_range, school_departamento as sc_dpto,
            mode() within group (order by school_native_any) as school_native_mode,
            avg(school_toilet_avg) as school_toilet_avg_avg
        from results.features_aggregate group by year_range, school_departamento),
    -- Feature imputation using window functions and joining with the previous tables
    imputed as (
        select a.year_range, a.student,
    -- STUDENT
        coalesce(age_avg, age_avg_avg) as age,
        coalesce(student_female, student_female_mode) as student_female,
        round(a.grado_code_avg) as grado_code, 
        coalesce(family_members_avg, avg(family_members_avg) over w_year) as family_members,
        -- If 'commute' is an empty string, substitute it by the most common commute that year
        case commute
        when '' then commute_mode
        else commute end as commute,
        -- If the student residence is missing, we assume they live where the school is. If the school location is missing one year, we use the data from the other years. Could be generalised by getting the most common departamento and municipio of students attending that school in that year, instead of the school details.
/*         departamento_count, */
        case departamento
            when '' then
                case school_departamento
                when '' then school_departamento_mode
                else school_departamento end
            else departamento end as departamento,
        case municipio
            when '' then
                case school_municipio
                when '' then school_municipio_mode
                else school_municipio end
            else municipio end as municipio,
    -- SCHOOL
/*         school_count, a.school, school_departamento_count, */
        case school_departamento
            when '' then school_departamento_mode
            else school_departamento end as school_departamento,
        case school_municipio
            when '' then school_municipio_mode
            else school_municipio end as school_municipio,
        coalesce(school_difference_avg, avg(school_difference_avg) over w_year) as school_difference,
/*         coalesce(school_difference_max, avg(school_difference_max) over w_year) as school_difference_max, */
/*         coalesce(school_difference_min, avg(school_difference_min) over w_year) as school_difference_min, */
        coalesce(school_mismatch_avg, avg(school_mismatch_avg) over w_year) as school_mismatch,
/*         coalesce(school_mismatch_max, avg(school_mismatch_max) over w_year) as school_mismatch_max, */
/*         coalesce(school_mismatch_min, avg(school_mismatch_min) over w_year) as school_mismatch_min, */
        coalesce(school_teachers_avg, avg(school_teachers_avg) over w_year) as school_teachers,
/*         coalesce(school_teachers_max, avg(school_teachers_max) over w_year) as school_teachers_max, */
/*         coalesce(school_teachers_min, avg(school_teachers_min) over w_year) as school_teachers_min, */
        coalesce(school_classrooms_avg, avg(school_classrooms_avg) over w_year) as school_classrooms,
/*         coalesce(school_classrooms_max, avg(school_classrooms_max) over w_year) as school_classrooms_max, */
/*         coalesce(school_classrooms_min, avg(school_classrooms_min) over w_year) as school_classrooms_min, */
    -- HOW DO WE IMPUTE INFORMATION THAT WE DID NOT HAVE AT THAT POINT IN TIME?
        coalesce(school_toilet_avg, school_toilet_avg_avg) as school_toilet,
/*         coalesce(school_toilet_max, avg(school_toilet_max) over w_year_region) as school_toilet_max, */
/*         coalesce(school_toilet_min, avg(school_toilet_min) over w_year_region) as school_toilet_min, */
        coalesce(school_native_any, school_native_any_mode, school_native_mode) as school_native,
/*         coalesce(school_native_all, school_native_all_mode) as school_native_all, */
        coalesce(school_rural_any, school_rural_any_mode) as school_rural,
/*         coalesce(school_rural_all, school_rural_all_mode) as school_rural_all, */
/*         coalesce(school_public_all, school_public_all_mode) as school_public_all, */
        coalesce(school_public_any, school_public_any_mode) as school_public
        from results.features_aggregate a
            left join group_by_year_grado b on a.year_range = b.year_range and round(a.grado_code_avg) = b.grado_code
            left join group_by_school c on a.school = c.school
            left join group_by_region d on a.year_range = d.year_range and a.school_departamento = d.sc_dpto
        window
            w_year as (partition by a.year_range)
/*             w_year_region as (partition by a.year_range, school_departamento) */
/*             w_year_grado as (partition by a.year_range, round(a.grado_code_avg)) */
    )
    select imputed.*,
        age - avg(age) over w_year_grado as overage1,
        age - avg(age) over w_year_grado_region as overage2,
        case when round(age) - avg(round(age)) over w_year_grado > 0 then 1 else 0 end as overage_bool,
        case when round(age) - avg(round(age)) over w_year_grado_region > 0 then 1 else 0 end as overage2_bool,
        coalesce((extract(year from lower(year_range))::int - extract(year from lower(lag(year_range) over w_student))::int)
        - (grado_code - lag(grado_code) over w_student), 0) as repeats,  -- Extracts the difference (in years) between the current date and the previous date, and substracts the difference in the grado_codes. If this is not 0, the student did not progress at a rate of one grado per year.
        case when (extract(year from lower(year_range))::int - extract(year from lower(lag(year_range) over w_student))::int)
        - (grado_code - lag(grado_code) over w_student) > 0 then 1 else 0 end as repeats_bool  -- creates a boolean from repeats
        from imputed
        window
            w_year_grado_region as (partition by year_range, school_departamento, round(grado_code)),
            w_year_grado as (partition by year_range, round(grado_code)),
            w_student as (partition by student order by year_range)
    );
create index imputed_year_range_student_idx on results.features_imputed(year_range, student);