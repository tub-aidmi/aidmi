{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    NULLIF(TRIM(c.firstname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(c.lastname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(c.email), '') AS "Email",
    NULLIF(TRIM(c.phone), '') AS "Phone",
    NULLIF(TRIM(c.title), '') AS "Title",
    CASE 
        WHEN LOWER(TRIM(c.role__c)) IN ('decision maker', 'decision_maker', 'dm') THEN 'Decision Maker'
        WHEN LOWER(TRIM(c.role__c)) IN ('end user', 'end_user', 'user') THEN 'End User'
        WHEN LOWER(TRIM(c.role__c)) IN ('technical contact', 'tech contact', 'technical') THEN 'Technical Contact'
        WHEN LOWER(TRIM(c.role__c)) IN ('executive sponsor', 'executive', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'DEU', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('EN', 'ENG', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('FR', 'FRA', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('ES', 'SPA', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('IT', 'ITA', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
