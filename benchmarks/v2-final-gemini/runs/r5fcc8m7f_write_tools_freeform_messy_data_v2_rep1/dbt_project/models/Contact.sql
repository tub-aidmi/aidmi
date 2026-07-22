{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(TRIM(lastname), 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(preferred_language__c)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(preferred_language__c)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c", -- Using source ID as the natural key
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source(source_name, source_table) }}
