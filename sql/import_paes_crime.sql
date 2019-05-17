############
### PAES ###
############


SET role el_salvador_mined_education_write;

CREATE TABLE raw.paes_14 (
	departmentos TEXT,
	municipios TEXT,
	nombre_centro_educativo TEXT,
	matematica float4,
	estudios_sociales_civica float4,
	languaje_literatura float4,
	ciencias_naturales float4,
	nota_global float4,
	year TEXT
	);

CREATE INDEX paes_year on raw.paes_14(year);
CREATE INDEX paes_depto on raw.paes_14(departmento);
CREATE INDEX paes_munic on raw.paes_14(municipio);


#############
### Crime ###
#############

SET role el_salvador_mined_education_write;

CREATE TABLE raw.crimenes_13_2009 (
	delito TEXT,
	crime_date TEXT,
	crime_hour TEXT,
	departmento TEXT,
	municipio TEXT,
	canton TEXT,
	caserio TEXT,
	sex varchar(20)
	);

CREATE TABLE raw.crimenes_13_2010 (
	cuenta TEXT,
	delito TEXT,
	crime_date TEXT,
	departmento TEXT,
	municipio TEXT,
	canton TEXT,
	sex varchar(20)
	);


CREATE TABLE raw.crimenes_13_2011 (
	cuenta TEXT,
	delito TEXT,
	crime_date TEXT,
	departmento TEXT,
	municipio TEXT,
	sex varchar(20)
	);

CREATE TABLE raw.crimenes_13_2012 (
	delito TEXT,
	crime_date TEXT,
	crime_hour TEXT,
	departmento TEXT,
	municipio TEXT,
	canton TEXT,
	area TEXT,
	sex varchar(20),
	age varchar(5),
	medium_used TEXT
	);


CREATE TABLE raw.crimenes_13_2013 (
	cuenta TEXT,
	delito TEXT,
	crime_date TEXT,
	departmento TEXT,
	municipio TEXT,
	area TEXT,
	weapon_type TEXT,
	age varchar(20),
	sex varchar(20),
	occupation TEXT
	); 


CREATE TABLE raw.crimenes_13_2014 (
	cuenta TEXT,
	departmento TEXT,
	municipio TEXT,
	area TEXT,
	crime_date TEXT,
	age varchar(20),
	sex varchar(20),
	weapon_type TEXT
	);

CREATE TABLE raw.crimenes_13_2015 (
	delito TEXT,
	crime_date TEXT,
	departmento TEXT,
	municipio TEXT,
	area TEXT,
	age varchar(20),
	sex varchar(20),
	occupation TEXT,
	weapon_type TEXT
	); 

CREATE TABLE raw.crimenes_13_2016(
	cuenta TEXT,
	crime_date TEXT,
	month varchar(10),
	day varchar(10),
	crime_hour TEXT,
	area TEXT,
	delito TEXT,
	departmento TEXT,
	municipio TEXT,
	canton TEXT,
	caserio TEXT,
	barrio TEXT,
	community TEXT,
	reparto TEXT,
	colonia TEXT, 
	no TEXT,
	calle TEXT,
	avenida TEXT,
	alameda TEXT,
	boulevard TEXT,
	pasaje TEXT, 
	direccion TEXT,
	sex varchar(20),
	age varchar(20),
	weapon_type TEXT,
	occupation TEXT
	); 


CREATE TABLE raw."13_crimenes" AS (
	SELECT
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	NULL as age,
	NULL as occupation,
	NULL as weapon_type
	FROM raw.crimenes_13_2009

	UNION ALL

	SELECT 
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	NULL as age,
	NULL as occupation,
	NULL as weapon_type
	FROM raw.crimenes_13_2010

	UNION ALL

	SELECT
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	NULL as age,
	NULL as occupation,
	NULL as weapon_type
	FROM raw.crimenes_13_2011

	UNION ALL

	SELECT
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	lower(btrim(age)) as age,
	NULL as occupation,
	lower(btrim(medium_used)) as weapon_type
	FROM raw.crimenes_13_2012

	UNION ALL

	SELECT
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	lower(btrim(age)) as age,
	lower(btrim(occupation)) as occupation,
	lower(btrim(weapon_type)) as weapon_type
	FROM raw.crimenes_13_2013

	UNION ALL
	
	SELECT
	NULL as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	lower(btrim(age)) as age,
	NULL as occupation,
	lower(btrim(weapon_type)) as weapon_type
	FROM raw.crimenes_13_2014

	UNION ALL

	SELECT
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	lower(btrim(age)) as age,
	lower(btrim(occupation)) as occupation,
	lower(btrim(weapon_type)) as weapon_type
	FROM raw.crimenes_13_2015

	UNION ALL

	SELECT	
	lower(btrim(delito)) as delito,
	to_date(crime_date, 'DD/MM/YYYY')::date as crime_date,
	lower(btrim(departmento)) as departmento,
	lower(btrim(municipio)) as municipio,
	nullif(lower(btrim(sex)), 'n/a') as sex,
	lower(btrim(age)) as age,
	lower(btrim(occupation)) as occupation,
	lower(btrim(weapon_type)) as weapon_type
	FROM raw.crimenes_13_2016
);

CREATE INDEX crimenes_date on raw."13_crimenes"(crime_date);
CREATE INDEX crimenes_depto on raw."13_crimenes"(departmento);
CREATE INDEX crimenes_munic on raw."13_crimenes"(municipio);



