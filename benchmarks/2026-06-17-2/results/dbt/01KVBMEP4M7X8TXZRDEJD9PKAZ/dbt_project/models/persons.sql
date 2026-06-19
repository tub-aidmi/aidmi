{{ config(materialized='table') }}

with contact as (
    select *
    from {{ source('src_01kvbmep4m7x8txzrdejd9pkaz_raw', 'contact') }}
    where is_deleted = false
),
account as (
    select *
    from {{ source('src_01kvbmep4m7x8txzrdejd9pkaz_raw', 'account') }}
    where is_deleted = false
)
select
    c.id as sf_contact_id,
    trim(concat_ws(' ', c.first_name, c.last_name)) as name,
    lower(trim(c.email)) as email,
    trim(coalesce(c.phone, c.mobile_phone)) as phone,
    trim(a.name) as org_name,
    case when c.owner_id ~ '^\d+$' then cast(c.owner_id as integer) else null end as owner_id,
    c.created_date as add_time
from contact c
left join account a on c.account_id = a.id