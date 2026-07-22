{{ config(materialized='table') }}

SELECT
    id AS "Id",
    TRIM(firstname) AS "FirstName",
    COALESCE(TRIM(lastname), 'Unknown') AS "LastName",
    TRIM(email) AS "Email",
    TRIM(phone) AS "Phone",
    TRIM(title) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    id AS "Legacy_Contact_ID__c", -- Using source id as legacy ID
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
