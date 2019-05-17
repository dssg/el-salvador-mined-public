split_part(upper(code), '.', 1) as code,
replace(replace(replace(replace(replace(lower(rural), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as rural,
CASE
WHEN lower(public) ~ '^pú' THEN 'public'
WHEN lower(public) ~ '^pri' THEN 'private'
END as public,
replace(replace(replace(replace(replace(lower(owner), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as owner,
replace(replace(replace(replace(replace(lower(dpto_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as dpto_name,
replace(replace(replace(replace(replace(lower(munic_name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as munic_name,
replace(replace(replace(replace(replace(lower(name), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as name,
replace(replace(replace(replace(replace(lower(address), 'ú', 'u'), 'ó', 'o'), 'í', 'i'), 'é', 'e'), 'á', 'a') as address,
case 
when lower(electricity_company) ~ '^s' then true
when lower(electricity_company) ~ '^n' then false
else electricity_company::bool end as electricity_company,

code is not null and upper(code) !~ 'TOTAL'  -- This gets rid of empty rows at the end