with source as (
    select * from {{ ref('client_contracts') }}
),

src_client_contracts as (
    select
        client_id,
        date(contract_start_date) as contract_start_date,
        contract_duration_months,
        spend_threshold,
        discounted_fee_margin

        
    from source
)

select * from src_client_contracts