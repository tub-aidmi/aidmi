{{ config(materialized='table') }}

with src as (
    select * from {{ source('fixture_master_v2_src', 'master_kontakte') }}
)

select
    -- Salesforce-style ID derived from source natural key
    SUBSTRING(MD5(kontakt_id), 1, 18) AS "Id",
    -- First Name
    INITCAP(TRIM(rufname)) AS "FirstName",
    -- Last Name (NOT NULL in target)
    CASE WHEN TRIM(familienname) IS NOT NULL AND TRIM(familienname) != ''
        THEN INITCAP(TRIM(familienname))
        ELSE 'Unknown'
    END AS "LastName",
    -- Email
    LOWER(TRIM(kontakt_email)) AS "Email",
    -- Phone
    tel AS "Phone",
    -- Title
    INITCAP(TRIM(berufsbezeichnung)) AS "Title",
    -- Role: map source rolle to allowed enum values
    CASE UPPER(TRIM(rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ANWENDER' THEN 'End User'
        WHEN 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN 'VORSTAND' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    -- Preferred Language: normalize to DE/EN/FR/ES/IT
    CASE UPPER(TRIM(korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'SPANISH' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        WHEN 'ITALIAN' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    -- AccountId: transform kd_nummer to same SFDC-style ID as Account.Id
    SUBSTRING(MD5(kd_nummer), 1, 18) AS "AccountId",
    -- Legacy Contact ID
    kontakt_id AS "Legacy_Contact_ID__c",
    -- CreatedDate
    CURRENT_DATE::text AS "CreatedDate",
    -- LastModifiedDate
    CURRENT_DATE::text AS "LastModifiedDate",
    -- IsDeleted
    0 AS "IsDeleted"
from src
