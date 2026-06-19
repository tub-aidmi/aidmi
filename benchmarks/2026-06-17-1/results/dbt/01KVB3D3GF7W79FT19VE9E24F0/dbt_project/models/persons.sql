{{ config(materialized='table') }}

with contacts as (
    select 
      id,
      account_id,
      owner_id,
      created_date,
      trim(coalesce(first_name, '') || ' ' || coalesce(last_name, '')) as full_name,
      lower(trim(email)) as clean_email,
      coalesce(phone, mobile_phone) as preferred_phone
    from {{ source('crm_raw', 'contact') }}
),
accounts as (
    select id, trim("name") as org_name 
    from {{ source('crm_raw', 'account') }}
)

select 
  c.id::text as sf_contact_id,
  c.full_name as name,
  c.clean_email as email,
  c.preferred_phone as phone,
  a.org_name,
  case when c.owner_id ~ '^\d+$' then c.owner_id::integer else null end as owner_id,
  c.created_date as add_time
from contacts c
left join accounts a on a.id = c.account_id
