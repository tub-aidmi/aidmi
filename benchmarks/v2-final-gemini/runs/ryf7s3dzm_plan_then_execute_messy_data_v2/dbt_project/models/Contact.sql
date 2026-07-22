{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(TRIM(lastname), 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN LOWER(role__c) IN ('decision maker', 'decision-maker') THEN 'Decision Maker'
        WHEN LOWER(role__c) IN ('end user', 'end-user') THEN 'End User'
        WHEN LOWER(role__c) IN ('technical contact', 'technical-contact') THEN 'Technical Contact'
        WHEN LOWER(role__c) IN ('executive sponsor', 'executive-sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(preferred_language__c) IN ('DE', 'GERMAN') THEN 'DE'
        WHEN UPPER(preferred_language__c) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(preferred_language__c) IN ('FR', 'FRENCH') THEN 'FR'
        WHEN UPPER(preferred_language__c) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(preferred_language__c) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
