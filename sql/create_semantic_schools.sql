set role el_salvador_mined_education_write;

drop table if exists staging.schools;

create table if not exists staging.schools as (
	with one as (
		select year_range as year, code, name, address, dpto_name, munic_name, native_region, rural,
		public, n_teachers as num_teachers, principal, principal_name, principal_surname,
		owner, owner_own, owner_borrow, owner_lease, owner_loan, owner_other,
		classrooms, electricity, electricity_company,
		toilet, toilet_works, toilet_no_works, toilet_pit, toilet_pit_no_works, toilet_pit_works,
		toilet_septic, toilet_septic_no_works, toilet_septic_works
		from cleaned."1_cleaned"
	),
	eight as (
		select year_range as year, school_id, point(x_coord, y_coord) as coordinates1,
		ST_MakePoint(x_coord, y_coord) as coordinates2,
		index, interventions, program_gendered_violence, program_pase, program_safe_school,
		vaso_de_leche, prostitution, rape, rape2, remoteness
		from cleaned."8_cleaned"
	),
	six_table as (
		select year_range as six_year, school as six_school, count(school) as six_student_count
		from cleaned."6_cleaned"
		group by six_year, six_school
	),
	two_table as (
		select year_range as two_year, school_id as two_school, count(school_id) as two_student_count
		from cleaned."2_cleaned"
		group by two_year, two_school
	),
    four as (
        select year_range as four_year, school_id as four_school, bool_and(ce_causas_desercion_presencia_pandillas) as ce_causas_desercion_presencia_pandillas, bool_and(ce_causas_desercion_incorporacion_trabajo) as ce_causas_desercion_incorporacion_trabajo, bool_and(factores_ce_drogas) as factores_ce_drogas, bool_and(factores_ce_extorsiones) as factores_ce_extorsiones, bool_and(factores_comunidad_explotacion_sexual) as factores_comunidad_explotacion_sexual, bool_and(ce_causas_desercion_violencia_pandilleril) as ce_causas_desercion_violencia_pandilleril, bool_and(factores_ce_violaciones) as factores_ce_violaciones, bool_and(ce_causas_desercion_violencia_sexual) as ce_causas_desercion_violencia_sexual, bool_and(factores_comunidad_maras) as factores_comunidad_maras, bool_and(factores_comunidad_trata) as factores_comunidad_trata, bool_and(ce_causas_desercion_embarazo) as ce_causas_desercion_embarazo, bool_and(ce_causas_desercion_migracion) as ce_causas_desercion_migracion, bool_and(factores_comunidad_violaciones) as factores_comunidad_violaciones, bool_and(factores_comunidad_armas_blancas) as factores_comunidad_armas_blancas, bool_and(factores_ce_maras) as factores_ce_maras, bool_and(ce_causas_desercion_acoso) as ce_causas_desercion_acoso, bool_and(ce_causas_desercion_prostitucion) as ce_causas_desercion_prostitucion, bool_and(factores_comunidad_robos) as factores_comunidad_robos from cleaned."4_cleanedriesgo" group by four_year, four_school
    ),
	stage as (
	select one.year as stage_year, one.code as stage_school_code, one.name as school_name, one.dpto_name as departamento, one.munic_name as municipio,
	eight.coordinates1 as coord1, eight.coordinates2 as coord2, one.address as address,
    case when one.year != '[2016-01-01, 2017-01-01)'::daterange then null::numeric
    else eight.index
    end as new_index,
	one.native_region as native_region, one.rural as rural, one.public as public, one.num_teachers as num_teachers,
	one.principal as principal, one.principal_name as principal_name, one.principal_surname as principal_surname,
	one.owner as owner, one.owner_own as own, one.owner_borrow as borrow, one.owner_lease as lease, one.owner_loan as loan,
	one.owner_other as own_other, one.classrooms as classrooms, one.electricity as electricity,
	one.electricity_company as electricity_company, one.toilet as toilet, one.toilet_works as toilet_works,
	one.toilet_no_works as toilet_no_works, one.toilet_pit as toilet_pit, one.toilet_pit_no_works as toilet_pit_no_works,
	one.toilet_pit_works as toilet_pit_works, one.toilet_septic as toilet_septic, one.toilet_septic_no_works as toilet_septic_no_works,
	one.toilet_septic_works as toilet_septic_works, eight.interventions as interventions, eight.program_gendered_violence as program_gendered_violence,
    eight.index,
	eight.program_pase as program_pase, eight.program_safe_school as program_safe_school, eight.vaso_de_leche as vaso_de_leche, eight.prostitution as prostitution,
	eight.rape as rape, eight.rape2 as rape2, now() as created_at
	from one left join eight on one.code = eight.school_id and one.year = eight.year
	order by one.code, one.year
	),
	stats as (
		select six_year as stats_year, six_school, @(two_student_count - six_student_count) as difference,
		cume_dist() over (partition by six_year order by @(two_student_count - six_student_count)) as census_mismatch
		from six_table
		left join two_table on six_school = two_school and six_table.six_year = two_table.two_year
	)
	select stats_year as year_range, stats.six_school as school, school_name, departamento, municipio,
	coord1, coord2, address, new_index, difference, census_mismatch, native_region, rural, public, num_teachers,
	principal, principal_name, principal_surname, owner, own, borrow, lease, loan, own_other, classrooms, electricity,
	electricity_company, toilet, toilet_works, toilet_no_works, toilet_pit, toilet_pit_works, toilet_pit_no_works, toilet_septic,
	toilet_septic_works, toilet_septic_no_works, interventions, program_gendered_violence, program_pase, program_safe_school,
	vaso_de_leche, prostitution, rape, rape2, 
    ce_causas_desercion_presencia_pandillas,ce_causas_desercion_incorporacion_trabajo, factores_ce_drogas, factores_ce_extorsiones, factores_comunidad_explotacion_sexual, ce_causas_desercion_violencia_pandilleril, factores_ce_violaciones, ce_causas_desercion_violencia_sexual, factores_comunidad_maras, factores_comunidad_trata, ce_causas_desercion_embarazo, ce_causas_desercion_migracion, factores_comunidad_violaciones, factores_comunidad_armas_blancas, factores_ce_maras, ce_causas_desercion_acoso, ce_causas_desercion_prostitucion, factores_comunidad_robos,
    created_at
	from stats 
        left join stage on stage_school_code = stats.six_school and stats_year = stage_year
        left join four on stats.six_school=four_school and stats_year = four_year
	order by stats.six_school, stats_year);
