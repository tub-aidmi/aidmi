-- dbt model for Contact
{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, '') AS "LastName", -- Target is NOT NULL, so coalesce to empty string if source is NULL
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN UPPER(TRIM(contact.role__c)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(contact.role__c)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(contact.role__c)) IN ('TECHNICAL CONTACT', 'TECHNIKER', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(contact.role__c)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('ES') THEN 'ES'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('IT') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c", -- Populate from source natural key
    NULL::text AS "CreatedDate", -- No source column available
    NULL::text AS "LastModifiedDate", -- No source column available
    0 AS "IsDeleted" -- Default to 0 as no source column available
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact