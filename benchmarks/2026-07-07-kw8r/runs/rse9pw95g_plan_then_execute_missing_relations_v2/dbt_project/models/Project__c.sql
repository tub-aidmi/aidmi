{{ config(materialized='table') }}

with src_project as (
    select * from {{ source('fixture_missing_relations_v2_src', 'project') }}
),
src_account as (
    select * from {{ source('fixture_missing_relations_v2_src', 'account') }}
),
src_opportunity as (
    select * from {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
)

select
    p.id::text as "Id",
    initcap(trim(p.name)) as "Name",
    case 
        when lower(trim(p.status)) in ('active', 'in_progress', 'working') then 'Active'
        when lower(trim(p.status)) in ('completed', 'done', 'finished', 'closed') then 'Completed'
        when lower(trim(p.status)) in ('planning', 'preparation') then 'In Planning'
        when lower(trim(p.status)) in ('on hold', 'paused', 'suspended') then 'On Hold'
        when lower(trim(p.status)) in ('cancelled', 'canceled', 'cancelled by customer') then 'Cancelled'
        else NULL 
    end as "Project_Status__c",
    case 
        when p.go_live is null or trim(p.go_live) = '' then NULL
        -- Try DD.MM.YYYY format (European) first
        when p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' then to_char(to_date(trim(p.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- Fallback for YYYY-MM-DD format
        when p.go_live ~ '^\d{4}-\d{2}-\d{2}$' then trim(p.go_live)
        else NULL 
    end as "Go_Live_Date__c",
    a.id::text as "Account__c",
    o.id::text as "Opportunity__c",
    p.id::text as "Legacy_Project_ID__c",
    current_timestamp::text as "CreatedDate",
    current_timestamp::text as "LastModifiedDate",
    0::integer as "IsDeleted"

from src_project p
left join src_account a 
    -- Clean keys by removing any leading non-alphanumeric characters (prefixes) and ensure matching types/format
    on regexp_replace(p.client_id, '^[^a-z0-9]+', '', 'i') = a.id::text
left join src_opportunity o
    on regexp_replace(p.opportunity_ref, '^[^a-z0-9]+', '', 'i') = o.id::text
