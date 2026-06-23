{{ config(materialized='table') }}

with contacts as (
    select *
    from {{ source('source_salesforce', 'contact') }}
),
accounts as (
    select *
    from {{ source('source_salesforce', 'account') }}
)
select
    c.id as sf_contact_id,
    trim(
        case
            when c.first_name is not null and c.last_name is not null then concat_ws(' ', c.first_name, c.last_name)
            when c.first_name is not null then c.first_name
            when c.last_name is not null then c.last_name
            else c.name
        end
    ) as name,
    lower(trim(c.email)) as email,
    trim(coalesce(c.phone, c.mobile_phone)) as phone,
    trim(a.name) as org_name,
    case when c.owner_id ~ '^\d+$' then c.owner_id::integer else null end as owner_id,
    c.created_date as add_time
from contacts c
left join accounts a on c.account_id = a.id
