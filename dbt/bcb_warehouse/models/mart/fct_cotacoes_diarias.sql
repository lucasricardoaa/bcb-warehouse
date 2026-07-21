{{
    config(
        materialized='table',
        description='Fato de cotações diárias: USD/BRL (série 1) e Selic (série 11). Granularidade: (serie_fk, data_fk).'
    )
}}

with indicadores as (
    select
        serie_id,
        data_referencia,
        valor,
        data_processamento
    from {{ ref('int_indicadores_economicos') }}
    where periodicidade = 'D'
),

dim_s as (
    select serie_sk, serie_id
    from {{ ref('dim_serie') }}
),

dim_d as (
    select data_sk, data
    from {{ ref('dim_data') }}
)

select
    {{ generate_surrogate_key(['i.serie_id', 'i.data_referencia']) }}  as cotacao_sk,
    s.serie_sk                                                          as serie_fk,
    d.data_sk                                                           as data_fk,
    i.valor,
    current_date                                                        as data_ingestao,
    i.data_processamento
from indicadores i
inner join dim_s s on i.serie_id = s.serie_id
inner join dim_d d  on i.data_referencia = d.data
