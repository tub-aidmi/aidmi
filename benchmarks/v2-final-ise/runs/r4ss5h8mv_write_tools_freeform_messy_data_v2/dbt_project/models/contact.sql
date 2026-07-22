{{ config(materialized='table') }}

SELECT
    CAST(id AS text) AS "Id",
    TRIM(firstname) AS "FirstName",
    COALESCE(TRIM(lastname), 'Unknown') AS "LastName",
    CASE
        WHEN email IS NULL THEN NULL
        WHEN LOWER(TRIM(email)) IN ('n/a', '') THEN NULL
        ELSE TRIM(email)
    END AS "Email",
    TRIM(phone) AS "Phone",
    TRIM(title) AS "Title",
    CASE 
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) = 'techniker' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(TRIM(preferred_language__c)) IN ('deutsch', 'german', 'de') THEN 'DE'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('english', 'englisch', 'en') THEN 'EN'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('french', 'français', 'fr') THEN 'FR'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('spanish', 'spanisch', 'es') THEN 'ES'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('italian', 'italienisch', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    CAST(id AS text) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
