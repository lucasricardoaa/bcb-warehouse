{{
    config(
        materialized='view',
        description='Taxa Selic acumulada no mês (% a.a.) — view sobre tabela Iceberg do Glue Job'
    )
}}

select
    serie_id,
    data                as data_referencia,
    valor               as taxa_selic_aa

from {{ source('bcb_warehouse_intermediate', 'selic') }}
