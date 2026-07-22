{{ config(materialized='table') }}

with chancen as (
    select * from {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

select
    cast(chance_id as text) as "Id",
    bezeichnung as "Name",
    case lower(trim(phase))
        when 'prospecting' then 'Prospecting'
        when 'qualifikation' then 'Qualification'
        when 'needs analysis' then 'Needs Analysis'
        when 'wertversprechen' then 'Value Proposition'
        when 'id. decision makers' then 'Id. Decision Makers'
        when 'perception analysis' then 'Perception Analysis'
        when 'proposal/price quote' then 'Proposal/Price Quote'
        when 'verhandlung' then 'Negotiation/Review'
        when 'gewonnen' then 'Closed Won'
        when 'verloren' then 'Closed Lost'
        else null
    end as "StageName",
    case
        when abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' then TO_DATE(abschlussdatum, 'DD.MM.YYYY')::text
        when abschlussdatum ~ '^\d{8}$' then SUBSTR(abschlussdatum, 1, 4) || '-' || SUBSTR(abschlussdatum, 5, 2) || '-' || SUBSTR(abschlussdatum, 7, 2)
        else null
    end as "CloseDate",
    volumen::DOUBLE PRECISION as "Amount",
    case upper(trim(waehrung))
        when 'EUR' then 'EUR'
        when 'USD' then 'USD'
        when 'GBP' then 'GBP'
        when 'CHF' then 'CHF'
        else waehrung
    end as "CurrencyIsoCode",
    '001' || SUBSTR(kd_nr, 2) as "AccountId",
    chance_id as "Legacy_Opportunity_ID__c",
    null::text as "CreatedDate",
    null::text as "LastModifiedDate",
    0 as "IsDeleted"

from chancen

where coalesce(bezeichnung, '') != ''   -- skip empty opportunities