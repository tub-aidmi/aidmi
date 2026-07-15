{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technical_contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'exec_sponsor') THEN 'Executive Sponsor'
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
    id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
WHERE TRIM(id) <> ''