{{ config(materialized='table') }}

select 
  id::text as sf_account_id,
  trim("name") as name,
  coalesce("billing_street", '') || ', ' || 
  coalesce("billing_city", '') || ', ' || 
  coalesce("billing_state", '') || ', ' || 
  coalesce("billing_postal_code", '') || ', ' || 
  coalesce("billing_country", '') as address,
  case when owner_id ~ '^\d+$' then owner_id::integer else null end as owner_id,
  created_date as add_time
from {{ source('crm_raw', 'account') }}
