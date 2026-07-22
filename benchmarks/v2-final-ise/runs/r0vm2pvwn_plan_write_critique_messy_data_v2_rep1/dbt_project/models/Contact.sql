{{ config(materialized='table') }}

SELECT 
    TRIM(id) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(lastname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner', 'tech lead', 'technical') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'entscheider', 'dm', 'buyer') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'benutzer', 'consumer') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor', 'executive', 'stakeholder') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(TRIM(preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}