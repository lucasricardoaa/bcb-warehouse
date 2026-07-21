{{
    config(
        materialized='table',
        description='Fato de indicadores mensais: IPCA (série 433). Granularidade: (serie_fk, ano, mes).'
    )
}}

with indicadores as (
    select
        serie_id,
        data_referencia,
        valor,
        data_processamento
    from {{ ref('int_indicadores_economicos') }}
    where periodicidade = 'M'
),

dim_s as (
    select serie_sk, serie_id
    from {{ ref('dim_serie') }}
)

select
    {{ generate_surrogate_key(['i.serie_id', 'i.data_referencia']) }}  as indicador_sk,
    s.serie_sk                                                          as serie_fk,
    year(i.data_referencia)                                             as ano,
    month(i.data_referencia)                                            as mes,
    i.valor,
    current_date                                                        as data_ingestao,
    i.data_processamento
from indicadores i
inner join dim_s s on i.serie_id = s.serie_id
