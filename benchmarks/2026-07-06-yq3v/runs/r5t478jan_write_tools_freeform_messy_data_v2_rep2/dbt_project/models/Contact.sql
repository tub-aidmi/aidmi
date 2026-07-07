{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(TRIM(lastname), 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN TRIM(INITCAP(role__c)) IN ('Decision Maker', 'Decision maker') THEN 'Decision Maker'
        WHEN TRIM(INITCAP(role__c)) IN ('End User', 'End user') THEN 'End User'
        WHEN TRIM(INITCAP(role__c)) IN ('Technical Contact', 'Technical contact') THEN 'Technical Contact'
        WHEN TRIM(INITCAP(role__c)) IN ('Executive Sponsor', 'Executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(preferred_language__c)) = 'GERMAN' THEN 'DE'
        WHEN TRIM(UPPER(preferred_language__c)) = 'ENGLISH' THEN 'EN'
        WHEN TRIM(UPPER(preferred_language__c)) = 'FRENCH' THEN 'FR'
        WHEN TRIM(UPPER(preferred_language__c)) = 'SPANISH' THEN 'ES'
        WHEN TRIM(UPPER(preferred_language__c)) = 'ITALIAN' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
