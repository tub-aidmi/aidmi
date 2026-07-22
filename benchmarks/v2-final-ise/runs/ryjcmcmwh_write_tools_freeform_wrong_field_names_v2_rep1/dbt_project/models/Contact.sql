{{ config(materialized='table') }}

select
    cast(ap_id as text) as "Id",
    case when trim(vorname) = '' then null else initcap(trim(vorname)) end as "FirstName",
    coalesce(initcap(trim(nachname)), 'Unknown') as "LastName",
    lower(trim(email_adresse)) as "Email",
    trim(telefonnummer) as "Phone",
    initcap(trim(position)) as "Title",
    case
        when upper(trim(funktion)) = 'DECISION MAKER' then 'Decision Maker'
        when upper(trim(funktion)) = 'END USER' then 'End User'
        when upper(trim(funktion)) = 'EXECUTIVE SPONSOR' then 'Executive Sponsor'
        when upper(trim(funktion)) = 'TECHNICAL CONTACT' then 'Technical Contact'
        else null
    end as "Role__c",
    case
        when upper(trim(sprache)) in ('DE', 'EN', 'FR', 'ES', 'IT') then upper(trim(sprache))
        else null
    end as "Preferred_Language__c",
    kunde as "AccountId",
    ap_id as "Legacy_Contact_ID__c",
    cast(null as text) as "CreatedDate",
    cast(null as text) as "LastModifiedDate",
    0 as "IsDeleted"
from {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
