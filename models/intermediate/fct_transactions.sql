with
    transactions as (
        select
            transaction_id,
            client_id,
            transaction_amount,
            transaction_type,
            transaction_date,
            platform_fee_margin,
            currency,
            linked_transaction_id

        from {{ ref("stg_transactions") }}
    ),

    currency_rates as (
        select currency, rate_date, exchange_rate_to_gbp

        from {{ ref("stg_currency_rates") }}
    ),

    contracts as (
        select
            client_id,
            contract_start_date,
            contract_duration_months,
            spend_threshold,
            discounted_fee_margin

        from {{ ref("stg_client_contracts") }}
    ),

    transaction_resolution as (
        select transaction_id, resolution_date, resolution_status

        from {{ ref("stg_transaction_resolutions") }}
    ),

    data_join as (
        select
            trn.transaction_id,
            trn.client_id,
            trn.transaction_amount,
            trn.transaction_type,
            trn.transaction_date,
            trn.platform_fee_margin,
            trn.currency,
            trn.linked_transaction_id,
            case
                when cr.exchange_rate_to_gbp is null then 1 else cr.exchange_rate_to_gbp
            end as exchange_rate,
            con.contract_start_date,
            con.contract_duration_months,
            con.spend_threshold,
            con.discounted_fee_margin,
            trnrsl.resolution_date,
            trnrsl.resolution_status

        from transactions as trn
        left join
            currency_rates as cr
            on (trn.currency = cr.currency and trn.transaction_date = cr.rate_date)
        left join contracts as con on trn.client_id = con.client_id
        left join
            transaction_resolution as trnrsl
            on trn.transaction_id = trnrsl.transaction_id
    )

select
    transaction_id,
    client_id,
    round((transaction_amount * exchange_rate), 2) as transaction_amount,
    transaction_type,
    transaction_date,
    platform_fee_margin,
    currency,
    linked_transaction_id,
    contract_start_date,
    contract_duration_months,
    spend_threshold,
    discounted_fee_margin,
    resolution_date,
    resolution_status

from data_join
