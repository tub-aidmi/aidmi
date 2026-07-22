{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    INITCAP(TRIM(COALESCE(lastname, 'Unknown'))) AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(preferred_language__c))
        ELSE NULL 
    END AS "Preferred_Language__c",
    TRIM(UPPER(accountid)) AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}