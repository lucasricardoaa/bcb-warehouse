{{
    config(
        materialized='table',
        description='Dimensão das séries BCB com surrogate key determinística (SHA-256, 32 chars).'
    )
}}

select
    {{ generate_surrogate_key(['serie_id']) }}      as serie_sk,
    serie_id,
    nome,
    unidade,
    periodicidade,
    fonte,
    cast(data_inicio_serie as date)                as data_inicio_serie
from {{ ref('dim_serie_seed') }}
