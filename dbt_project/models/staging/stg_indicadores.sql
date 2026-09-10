{{ config(materialized='view') }}

with normalized as (
	select
		toUInt32(codigo_serie) as codigo_serie,
		trimBoth(nome_serie) as nome_serie,
		toDate(data_referencia) as data_referencia,
		toFloat64(valor) as valor,
		toDateTime(extraido_em) as extraido_em
	from {{ source('pipeline_raw', 'raw_indicadores') }}
	where data_referencia is not null
)

select
	codigo_serie,
	any(nome_serie) as nome_serie,
	data_referencia,
	argMax(valor, extraido_em) as valor,
	max(extraido_em) as ultimo_extraido_em
from normalized
group by
	codigo_serie,
	data_referencia
 