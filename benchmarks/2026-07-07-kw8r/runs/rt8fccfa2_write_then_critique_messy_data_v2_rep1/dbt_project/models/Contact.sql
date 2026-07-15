{{ config(materialized='table') }}
SELECT 
    c.id AS "Id",
    TRIM(c.firstname) AS "FirstName",
    COALESCE(NULLIF(TRIM(c.lastname), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(c.title) AS "Title",
    CASE 
        WHEN TRIM(LOWER(c.role__c)) IN ('decision maker', 'decision_maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(c.role__c)) IN ('end user', 'end_user') THEN 'End User'
        WHEN TRIM(LOWER(c.role__c)) IN ('technical contact', 'technical_contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(c.role__c)) IN ('executive sponsor', 'executive_sponsor') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('de', 'german') THEN 'DE'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('en', 'english') THEN 'EN'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('fr', 'french') THEN 'FR'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('es', 'spanish') THEN 'ES'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('it', 'italian') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c