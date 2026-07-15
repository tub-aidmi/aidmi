{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    TRIM(INITCAP(firstname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE LOWER(TRIM(role__c))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(preferred_language__c))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}