select
    codigo_serie,
    any(nome_serie) as nome_serie,
    toStartOfMonth(data_referencia) as mes_referencia,
    count() as quantidade_registros,
    avg(valor) as valor_medio,
    min(valor) as valor_minimo,
    max(valor) as valor_maximo,
    argMax(valor, data_referencia) as ultimo_valor,
    max(ultimo_extraido_em) as ultimo_extraido_em
from {{ ref('stg_indicadores') }}
group by
    codigo_serie,
    mes_referencia