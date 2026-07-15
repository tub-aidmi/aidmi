{{ config(materialized='table') }}

with key_normalization as (
    -- Extract digits from source keys to handle format mismatches across tables (e.g. "K-1234" vs "1234")
    select
        cast(regexp_replace(kundennummer, '[^0-9]', '', 'g') as integer) as kunden_num_numeric
    from {{ source('fixture_master_v2_src', 'master_kunden') }}
),

project_keyed as (
    select
        p.*,
        kn.kunden_num_numeric as account_key_numeric
    from {{ source('fixture_master_v2_src', 'master_projekte') }} p
    left join key_normalization kn
        on cast(regexp_replace(p.kunden_kennung, '[^0-9]', '', 'g') as integer) = kn.kunden_num_numeric
),

opportunity_keyed as (
    select
        o.*,
        cast(regexp_replace(o.kunden_ref, '[^0-9]', '', 'g') as integer) as opp_account_key_numeric
    from {{ source('fixture_master_v2_src', 'master_opportunities') }} o
),

id_mappings as (
    -- Pre-compute Salesforce-style IDs for Accounts and Opportunities so Project__c can reference them consistently
    select
        '001' || lpad(kunden_num_numeric::text, 15, '0') as account_id
    from key_normalization

    union all

    select
        '006' || lpad(cast(regexp_replace(opp_kennung, '[^0-9]', '', 'g') as integer)::text, 15, '0') as opportunity_id
    from {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

account_lookup as (
    select account_id, kunden_num_numeric
    from id_mappings
),

opportunity_lookup as (
    select opportunity_id, opp_kennung
    from id_mappings
)

select
    -- Id: Salesforce-style 18-char ID for Project__c (custom object prefix '00D')
    '00D' || lpad(ltrim(projekt_kennung, '0')::integer::text, 15, '0') as "Id",

    -- Name
    trim(coalesce(projektname, '')) as "Name",

    -- Project_Status__c: map German status terms to declared enum values
    case lower(trim(projektstatus))
        when 'aktiv' then 'Active'
        when 'abgeschlossen' then 'Completed'
        when 'in planung' then 'In Planning'
        when 'angehalten' then 'On Hold'
        when 'storniert' then 'Cancelled'
        when 'active' then 'Active'
        when 'completed' then 'Completed'
        when 'cancelled' then 'Cancelled'
        else null
    end as "Project_Status__c",

    -- Go_Live_Date__c: parse DD.MM.YYYY or YYYY-MM-DD, output ISO text or NULL
    case
        when go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' then to_char(to_date(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        when go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' then go_live_datum
        else null
    end as "Go_Live_Date__c",

    -- Account__c: reference Salesforce Account.Id (not raw customer number)
    al.account_id as "Account__c",

    -- Opportunity__c: reference Salesforce Opportunity.Id
    ol.opportunity_id as "Opportunity__c",

    -- Legacy_Project_ID__c: preserve original natural key
    projekt_kennung as "Legacy_Project_ID__c",

    -- Audit fields — default values for a staging-to-SFDC migration
    cast(date_trunc('day', current_timestamp) as text) as "CreatedDate",
    cast(date_trunc('day', current_timestamp) as text) as "LastModifiedDate",
    0 as "IsDeleted"

from project_keyed p
left join account_lookup al on al.kunden_num_numeric = p.account_key_numeric
left join opportunity_lookup ol on ol.opp_kennung = p.opp_kennung_ref