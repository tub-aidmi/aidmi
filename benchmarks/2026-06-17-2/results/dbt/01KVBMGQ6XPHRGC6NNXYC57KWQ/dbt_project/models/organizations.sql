{{ config(materialized='table') }}

select
    a.id as sf_account_id,
    trim(a.name) as name,
    trim(
        concat_ws(', ',
            a.billing_street,
            a.billing_city,
            a.billing_state,
            a.billing_postal_code,
            a.billing_country
        )
    ) as address,
    case when a.owner_id ~ '^\d+$' then a.owner_id::integer else null end as owner_id,
    a.created_date as add_time
from {{ source('source_salesforce', 'account') }} a
