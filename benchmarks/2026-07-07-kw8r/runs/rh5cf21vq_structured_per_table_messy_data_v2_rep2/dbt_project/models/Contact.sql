{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(regexp_replace(role__c, '\s+', ' ', 'g')) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(regexp_replace(role__c, '\s+', ' ', 'g')) = 'end user' THEN 'End User'
        WHEN LOWER(regexp_replace(role__c, '\s+', ' ', 'g')) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(regexp_replace(role__c, '\s+', ' ', 'g')) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(preferred_language__c))
        WHEN LOWER(regexp_replace(preferred_language__c, '\s+', ' ', 'g')) = 'german' THEN 'DE'
        WHEN LOWER(regexp_replace(preferred_language__c, '\s+', ' ', 'g')) = 'english' THEN 'EN'
        WHEN LOWER(regexp_replace(preferred_language__c, '\s+', ' ', 'g')) = 'french' THEN 'FR'
        WHEN LOWER(regexp_replace(preferred_language__c, '\s+', ' ', 'g')) = 'spanish' THEN 'ES'
        WHEN LOWER(regexp_replace(preferred_language__c, '\s+', ' ', 'g')) = 'italian' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}