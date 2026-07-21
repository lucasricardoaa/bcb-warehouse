{{
    config(
        materialized='table',
        description='Dimensão calendário 2020-2030 com indicadores de dia útil e feriado nacional.'
    )
}}

with feriados as (
    select cast(data as date) as data
    from {{ ref('feriados_nacionais') }}
),

base as (
    select
        {{ generate_surrogate_key(['data']) }}          as data_sk,
        cast(data as date)                             as data,
        ano,
        mes,
        mes_nome,
        trimestre,
        semestre,
        dia_da_semana,
        dia_da_semana_nome
    from {{ ref('dim_data_seed') }}
)

select
    b.data_sk,
    b.data,
    b.ano,
    b.mes,
    b.mes_nome,
    b.trimestre,
    b.semestre,
    b.dia_da_semana,
    b.dia_da_semana_nome,
    (f.data is not null)                             as eh_feriado_nacional,
    (b.dia_da_semana between 1 and 5
        and f.data is null)                          as eh_dia_util
from base b
left join feriados f
    on b.data = f.data
