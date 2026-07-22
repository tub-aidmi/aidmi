{{ config(materialized='table') }}

with key_normalization as (
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

account_lookup as (
    select
        '001' || lpad(kunden_num_numeric::text, 15, '0') as account_id,
        kunden_num_numeric
    from key_normalization
),

opportunity_lookup as (
    select
        '006' || lpad(cast(regexp_replace(opp_kennung, '[^0-9]', '', 'g') as integer)::text, 15, '0') as opportunity_id,
        opp_kennung
    from {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

select
    '00D' || lpad(cast(regexp_replace(projekt_kennung, '[^0-9]', '', 'g') as integer)::text, 15, '0') as "Id",
    trim(coalesce(projektname, '')) as "Name",
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
    case
        when go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' then to_char(to_date(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        when go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' then go_live_datum
        else null
    end as "Go_Live_Date__c",
    al.account_id as "Account__c",
    ol.opportunity_id as "Opportunity__c",
    projekt_kennung as "Legacy_Project_ID__c",
    cast(date_trunc('day', current_timestamp) as text) as "CreatedDate",
    cast(date_trunc('day', current_timestamp) as text) as "LastModifiedDate",
    0 as "IsDeleted"

from project_keyed p
left join account_lookup al on al.kunden_num_numeric = p.account_key_numeric
left join opportunity_lookup ol on ol.opp_kennung = p.opp_kennung_ref