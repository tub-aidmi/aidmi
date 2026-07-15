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
    case
        when volumen is null then null
        when regexp_replace(volumen::text, '[^\d.,-]', '') ~ '^[0-9]+\.[0-9]{1,3},[0-9]{2}$' then 
            REGEXP_REPLACE(REGEXP_REPLACE(volumen::text, '[^\d.,-]', ''), '^([0-9]+)\.([0-9]{3}),([0-9]{2})$', '\1\2.\3')::DOUBLE PRECISION
        when volumen ~ '^\d+\.\d{3},\d{2}$' then 
            REGEXP_REPLACE(REGEXP_REPLACE(volumen, '[^\d.,]', ''), '^([0-9]+)\.([0-9]{3}),([0-9]{2})$', '\1\2.\3')::DOUBLE PRECISION
        else volumen::DOUBLE PRECISION
    end as "Amount",
    case upper(trim(waehrung))
        when 'EUR' then 'EUR'
        when 'USD' then 'USD'
        when 'GBP' then 'GBP'
        when 'CHF' then 'CHF'
        else waehrung
    end as "CurrencyIsoCode",
    -- AccountId: derive from customer number (kd_nr) using same transformation as kunden_nr -> Account.Id
    -- Typically: remove K prefix, standardize format to match Salesforce Id pattern
    -- Since we cannot use ref(), transform kd_nr consistently with how kunden_nr maps to Account.Id
    '001' || SUBSTR(kd_nr, 2) as "AccountId",
    chance_id as "Legacy_Opportunity_ID__c",
    null::text as "CreatedDate",
    null::text as "LastModifiedDate",
    0 as "IsDeleted"

from chancen

where coalesce(bezeichnung, '') != ''  -- skip empty opportunities