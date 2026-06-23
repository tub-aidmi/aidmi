{{ config(materialized='table') }}

select
    c.id::text as sf_contact_id,
    trim(concat(c.first_name, ' ', c.last_name)) as name,
    lower(trim(c.email)) as email,
    coalesce(c.phone, c.mobile_phone) as phone,
    a.name as org_name,
    null::integer as owner_id,
    c.created_date as add_time
from {{ source('src_01kvbnt66b36h04ksx4n4ccdqe_raw', 'contact') }} c
left join {{ source('src_01kvbnt66b36h04ksx4n4ccdqe_raw', 'account') }} a
    on a.id = c.account_id
where coalesce(c.is_deleted, false) = false