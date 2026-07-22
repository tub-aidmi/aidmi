{{ config(materialized='table') }}

SELECT
    id AS "Id",
    TRIM(INITCAP(firstname)) AS "FirstName",
    COALESCE(NULLIF(TRIM(INITCAP(lastname)), ''), 'Unknown') AS "LastName",
    TRIM(LOWER(email)) AS "Email",
    TRIM(phone) AS "Phone",
    TRIM(INITCAP(title)) AS "Title",
    CASE 
        WHEN TRIM(LOWER(role__c)) IN ('decision maker', 'dm', 'decision_maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(role__c)) IN ('end user', 'eu', 'end_user') THEN 'End User'
        WHEN TRIM(LOWER(role__c)) IN ('technical contact', 'tc', 'technical_contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(role__c)) IN ('executive sponsor', 'es', 'executive_sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(UPPER(preferred_language__c)) IN ('DE', 'GERMAN') THEN 'DE'
        WHEN TRIM(UPPER(preferred_language__c)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN TRIM(UPPER(preferred_language__c)) IN ('FR', 'FRENCH') THEN 'FR'
        WHEN TRIM(UPPER(preferred_language__c)) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN TRIM(UPPER(preferred_language__c)) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
