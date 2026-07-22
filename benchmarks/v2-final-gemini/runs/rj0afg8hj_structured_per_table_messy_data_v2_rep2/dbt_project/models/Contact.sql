-- {{ config(materialized='table') }}

SELECT
    id AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(role__c) IN ('decision maker') THEN 'Decision Maker'
        WHEN LOWER(role__c) IN ('end user') THEN 'End User'
        WHEN LOWER(role__c) IN ('technical contact', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(role__c) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(preferred_language__c) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(preferred_language__c) IN ('en', 'englisch') THEN 'EN'
        WHEN LOWER(preferred_language__c) IN ('fr', 'französisch') THEN 'FR'
        WHEN LOWER(preferred_language__c) = 'es' THEN 'ES'
        WHEN LOWER(preferred_language__c) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}