-- cleaning some columns of 4_joinedriesgo
case 
	when factores_comunidad_robos = '1' then true
	when factores_comunidad_robos = '1.0' then true
	when factores_comunidad_robos = '2' then false
	when factores_comunidad_robos = '2.0' then false
	when factores_comunidad_robos = '0' then null::bool
	end as factores_comunidad_robos,
case 
	when ce_causas_desercion_prostitucion = '1' then true
	when ce_causas_desercion_prostitucion = '1.0' then true
	when ce_causas_desercion_prostitucion = '2' then false
	when ce_causas_desercion_prostitucion = '2.0' then false
	when ce_causas_desercion_prostitucion = '0' then null::bool
	end as ce_causas_desercion_prostitucion,
case 
	when ce_causas_desercion_acoso = '1' then true
	when ce_causas_desercion_acoso = '1.0' then true
	when ce_causas_desercion_acoso = '2' then false
	when ce_causas_desercion_acoso = '2.0' then false
	when ce_causas_desercion_acoso = '0' then null::bool
	end as ce_causas_desercion_acoso,
case 
	when factores_ce_maras = '1' then true
	when factores_ce_maras = '1.0' then true
	when factores_ce_maras = '2' then false
	when factores_ce_maras = '2.0' then false
	when factores_ce_maras = '0' then null::bool
	end as factores_ce_maras,
case 
	when factores_comunidad_armas_blancas = '1' then true
	when factores_comunidad_armas_blancas = '1.0' then true
	when factores_comunidad_armas_blancas = '2' then false
	when factores_comunidad_armas_blancas = '2.0' then false
	when factores_comunidad_armas_blancas = '0' then null::bool
	end as factores_comunidad_armas_blancas,
case 
	when factores_comunidad_violaciones = '1' then true
	when factores_comunidad_violaciones = '1.0' then true
	when factores_comunidad_violaciones = '2' then false
	when factores_comunidad_violaciones = '2.0' then false
	when factores_comunidad_violaciones = '0' then null::bool
	end as factores_comunidad_violaciones,
case 
	when ce_causas_desercion_migracion = '1' then true
	when ce_causas_desercion_migracion = '1.0' then true
	when ce_causas_desercion_migracion = '2' then false
	when ce_causas_desercion_migracion = '2.0' then false
	when ce_causas_desercion_migracion = '0' then null::bool
	end as ce_causas_desercion_migracion,
case 
	when ce_causas_desercion_embarazo = '1' then true
	when ce_causas_desercion_embarazo = '1.0' then true
	when ce_causas_desercion_embarazo = '2' then false
	when ce_causas_desercion_embarazo = '2.0' then false
	when ce_causas_desercion_embarazo = '0' then null::bool
	end as ce_causas_desercion_embarazo,
case 
	when factores_comunidad_trata = '1' then true
	when factores_comunidad_trata = '1.0' then true
	when factores_comunidad_trata = '2' then false
	when factores_comunidad_trata = '2.0' then false
	when factores_comunidad_trata = '0' then null::bool
	end as factores_comunidad_trata,
case 
	when factores_comunidad_maras = '1' then true
	when factores_comunidad_maras = '1.0' then true
	when factores_comunidad_maras = '2' then false
	when factores_comunidad_maras = '2.0' then false
	when factores_comunidad_maras = '0' then null::bool
	end as factores_comunidad_maras,
case 
	when ce_causas_desercion_violencia_sexual = '1' then true
	when ce_causas_desercion_violencia_sexual = '1.0' then true
	when ce_causas_desercion_violencia_sexual = '2' then false
	when ce_causas_desercion_violencia_sexual = '2.0' then false
	when ce_causas_desercion_violencia_sexual = '0' then null::bool
	end as ce_causas_desercion_violencia_sexual,
case 
	when factores_ce_violaciones = '1' then true
	when factores_ce_violaciones = '1.0' then true
	when factores_ce_violaciones = '2' then false
	when factores_ce_violaciones = '2.0' then false
	when factores_ce_violaciones = '0' then null::bool
	end as factores_ce_violaciones,
case 
	when ce_causas_desercion_violencia_pandilleril = '1' then true
	when ce_causas_desercion_violencia_pandilleril = '1.0' then true
	when ce_causas_desercion_violencia_pandilleril = '2' then false
	when ce_causas_desercion_violencia_pandilleril = '2.0' then false
	when ce_causas_desercion_violencia_pandilleril = '0' then null::bool
	end as ce_causas_desercion_violencia_pandilleril,
case 
	when factores_comunidad_explotacion_sexual = '1' then true
	when factores_comunidad_explotacion_sexual = '1.0' then true
	when factores_comunidad_explotacion_sexual = '2' then false
	when factores_comunidad_explotacion_sexual = '2.0' then false
	when factores_comunidad_explotacion_sexual = '0' then null::bool
	end as factores_comunidad_explotacion_sexual,
case 
	when factores_ce_extorsiones = '1' then true
	when factores_ce_extorsiones = '1.0' then true
	when factores_ce_extorsiones = '2' then false
	when factores_ce_extorsiones = '2.0' then false
	when factores_ce_extorsiones = '0' then null::bool
	end as factores_ce_extorsiones,
split_part(trim(both '"' from upper(id)), '.', 1) as school_id,	
case 
	when factores_ce_drogas = '1' then true
	when factores_ce_drogas = '1.0' then true
	when factores_ce_drogas = '2' then false
	when factores_ce_drogas = '2.0' then false
	when factores_ce_drogas = '0' then null::bool
	end as factores_ce_drogas,
case 
	when ce_causas_desercion_incorporacion_trabajo = '1' then true
	when ce_causas_desercion_incorporacion_trabajo = '1.0' then true
	when ce_causas_desercion_incorporacion_trabajo = '2' then false
	when ce_causas_desercion_incorporacion_trabajo = '2.0' then false
	when ce_causas_desercion_incorporacion_trabajo = '0' then null::bool
	end as ce_causas_desercion_incorporacion_trabajo,
case 
	when ce_causas_desercion_presencia_pandillas = '1' then true
	when ce_causas_desercion_presencia_pandillas = '1.0' then true
	when ce_causas_desercion_presencia_pandillas = '2' then false
	when ce_causas_desercion_presencia_pandillas = '2.0' then false
	when ce_causas_desercion_presencia_pandillas = '0' then null::bool
	when ce_causas_desercion_presencia_pandillas = '1. Si' then true
	when ce_causas_desercion_presencia_pandillas = '2. No' then false	
	when ce_causas_desercion_presencia_pandillas = '3. NS/NR' then null::bool	
	end as ce_causas_desercion_presencia_pandillas,

id != '0' and id is not null