CASE status 
WHEN 'activo' THEN '1'::bool 
WHEN 'inactivo' THEN '0'::bool 
WHEN '1' then '1'::bool 
WHEN '0' then '0'::bool 
END as status,
CASE WHEN 
to_date(birth_date, 'DD-MM-YY') > to_date(year, 'YYYY') - INTERVAL '2 YEAR' THEN NULL::date
ELSE to_date(birth_date, 'DD-MM-YY') END as birth_date,
CASE gender 
WHEN 'Masculino' THEN 'm'::char(1) 
WHEN 'Femenino' THEN 'f'::char(1) END as gender,
CASE grado 
WHEN '4P PARVULARIA 4 AÑOS' THEN -2
WHEN '5P PARVULARIA 5 AÑOS' THEN -1
WHEN '6P PARVULARIA 6 AÑOS' THEN 0
WHEN '01 PRIMER GRADO' THEN 1
WHEN '02 SEGUNDO GRADO' THEN 2
WHEN '03 TERCER GRADO' THEN 3
WHEN '04 CUARTO GRADO' THEN 4
WHEN '05 QUINTO GRADO' THEN 5
WHEN '06 SEXTO GRADO' THEN 6
WHEN '07 SEPTIMO GRADO' THEN 7
WHEN '08 OCTAVO GRADO' THEN 8
WHEN '09 NOVENO GRADO' THEN 9
WHEN '1B PRIMER AÑO DE BACHILLERATO' THEN 10
WHEN 'S1 SECCIÓN 1 AÑO' THEN 10
WHEN 'S2 SECCIÓN 2 AÑO' THEN 11
WHEN '2B SEGUNDO AÑO DE BACHILLERATO' THEN 11
WHEN '3B TERCER AÑO DE BACHILLERATO' THEN 12
WHEN '4B CUARTO AÑO DE BACHILLERATO' THEN 13 
END as grado_code,
split_part(upper(school), '.', 1) as school,
replace(replace(replace(replace(replace(lower(address), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as address,
replace(replace(replace(replace(replace(lower(responsable), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as responsable,
replace(replace(replace(replace(replace(lower(father), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as father,
replace(replace(replace(replace(replace(lower(mother), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as mother,
replace(replace(replace(replace(replace(lower(dpto_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as dpto_name,
replace(replace(replace(replace(replace(lower(munic_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as munic_name,
replace(replace(replace(replace(replace(lower(medio_transporte), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as medio_transporte,
replace(replace(replace(replace(replace(lower(school_dpto_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as school_dpto_name,
replace(replace(replace(replace(replace(lower(school_munic_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as school_munic_name,
replace(replace(replace(replace(replace(lower(school_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as school_name,
case when family_members::numeric > 20 then null::smallint else family_members::smallint end as family_members,

id != '0' and id is not null