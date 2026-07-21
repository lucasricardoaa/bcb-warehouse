{{
    config(
        materialized='view',
        description='IPCA variação mensal (%) — view sobre tabela Iceberg do Glue Job'
    )
}}

select
    serie_id,
    data                as data_referencia,
    valor               as variacao_mensal_pct

from {{ source('bcb_warehouse_intermediate', 'ipca') }}
