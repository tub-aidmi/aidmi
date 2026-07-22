{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    TRIM(c.firstname) AS "FirstName",
    TRIM(c.lastname) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(c.title) AS "Title",
    CASE 
        WHEN LOWER(TRIM(c.role__c)) IN ('decision maker', 'decision_maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(c.role__c)) IN ('end user', 'end_user') THEN 'End User'
        WHEN LOWER(TRIM(c.role__c)) IN ('technical contact', 'technical_contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(c.role__c)) IN ('executive sponsor', 'executive_sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(c.preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c