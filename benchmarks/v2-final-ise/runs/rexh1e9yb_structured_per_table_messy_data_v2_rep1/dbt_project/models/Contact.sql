{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    firstname,
    INITCAP(TRIM(lastname)) AS "LastName",
    CASE WHEN TRIM(COALESCE(email, '')) = '' OR TRIM(COALESCE(email, '')) = 'N/A' THEN NULL ELSE TRIM(COALESCE(email, '')) END AS "Email",
    phone,
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'decisionmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'endanwender', 'enduser') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(preferred_language__c)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('fr', 'french', 'français', 'franösisch') THEN 'FR'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }}