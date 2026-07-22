{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    NULLIF(TRIM(c.firstname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(c.lastname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(c.email), '') AS "Email",
    NULLIF(TRIM(c.phone), '') AS "Phone",
    NULLIF(TRIM(c.title), '') AS "Title",
    CASE 
        WHEN UPPER(TRIM(c.role__c)) IN ('DECISION MAKER', 'DECISION_MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(c.role__c)) IN ('END USER', 'END_USER') THEN 'End User'
        WHEN UPPER(TRIM(c.role__c)) IN ('TECHNICAL CONTACT', 'TECHNICAL_CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(c.role__c)) IN ('EXECUTIVE SPONSOR', 'EXECUTIVE_SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') 
        THEN UPPER(TRIM(c.preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    NULLIF(TRIM(c.id), '') AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
