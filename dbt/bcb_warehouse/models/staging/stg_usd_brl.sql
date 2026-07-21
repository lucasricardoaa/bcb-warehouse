{{
    config(
        materialized='view',
        description='Taxa de câmbio USD/BRL diária — view sobre tabela Iceberg do Glue Job'
    )
}}

select
    serie_id,
    data                as data_referencia,
    valor               as taxa_cambio_brl_usd

from {{ source('bcb_warehouse_intermediate', 'usd_brl') }}
