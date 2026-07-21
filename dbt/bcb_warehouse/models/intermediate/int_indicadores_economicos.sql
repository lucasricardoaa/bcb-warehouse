{{
    config(
        materialized='table',
        description='Unificação das três séries BCB em formato longo normalizado. Entrada para os modelos mart.'
    )
}}

with cotacoes as (
    select
        serie_id,
        data_referencia,
        taxa_cambio_brl_usd  as valor,
        'D'                  as periodicidade
    from {{ ref('stg_usd_brl') }}
),

selic as (
    select
        serie_id,
        data_referencia,
        taxa_selic_aa        as valor,
        'D'                  as periodicidade
    from {{ ref('stg_selic') }}
),

ipca as (
    select
        serie_id,
        data_referencia,
        variacao_mensal_pct  as valor,
        'M'                  as periodicidade
    from {{ ref('stg_ipca') }}
),

unificado as (
    select * from cotacoes
    union all
    select * from selic
    union all
    select * from ipca
)

select
    serie_id,
    data_referencia,
    valor,
    periodicidade,
    current_timestamp        as data_processamento
from unificado
