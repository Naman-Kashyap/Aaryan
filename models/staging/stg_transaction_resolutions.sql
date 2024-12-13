with source as (
    select * from {{ ref('transaction_resolutions') }}
),

src_transaction_resolutions as (
    select
        transaction_id,
        PARSE_DATE('%d/%m/%Y', resolution_date) as resolution_date,
        resolution_status

        
    from source
)

select * from src_transaction_resolutions