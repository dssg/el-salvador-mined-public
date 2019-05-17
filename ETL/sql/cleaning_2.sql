CASE repeats
WHEN 'Si' THEN TRUE
WHEN 'No' THEN FALSE
WHEN '2' THEN FALSE
WHEN '1' THEN TRUE
WHEN '0'  THEN NULL::BOOL
WHEN 'No especificado' THEN NULL::BOOL
END as repeats,
CASE overage
WHEN 'Si' THEN TRUE
WHEN 'No' THEN FALSE
WHEN '2' THEN FALSE
WHEN '1' THEN TRUE
WHEN '0'  THEN NULL::BOOL
WHEN 'No especificado' THEN NULL::BOOL
END as overage,
CASE gender 
WHEN 'Masculino' THEN 'M'::char(1)
WHEN '1' THEN 'M'::char(1)
WHEN 'Femenino' THEN 'F'::char(1)
WHEN '2' THEN 'F'::char(1)
WHEN 'No especificado' THEN NULL::char(1)
END as gender,
CASE
WHEN lower(trim(both '"' from school_urban)) ~ '^urb' THEN 'urban'
WHEN lower(trim(both '"' from school_urban)) ~ '^ru' THEN 'rural'
END as school_rural,
CASE
WHEN lower(trim(both '"' from school_public)) ~ '^pú' THEN 'public'
WHEN lower(trim(both '"' from school_public)) ~ '^pri' THEN 'private'
END as school_public,
trim(both '"' from school_name) as school_name,
split_part(trim(both '"' from upper(school_id)), '.', 1) as school_id,
trim(both '"' from grado_id) as grado_id,
trim(both '"' from school_dpto) as school_dpto,
trim(both '"' from birth_certificate) as birth_certificate,
trim(both '"' from school_munic_name) as school_munic_name,
trim(both '"' from school_modalidad_id) as school_modalidad_id,
trim(both '"' from school_munic) as school_munic,
trim(both '"' from nivel_id) as nivel_id,
trim(both '"' from school_dpto_name) as school_dpto_name,