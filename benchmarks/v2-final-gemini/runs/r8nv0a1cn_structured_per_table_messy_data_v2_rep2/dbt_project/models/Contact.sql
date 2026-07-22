{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    TRIM(src.firstname) AS "FirstName",
    COALESCE(TRIM(src.lastname), '') AS "LastName",
    TRIM(src.email) AS "Email",
    TRIM(src.phone) AS "Phone",
    TRIM(src.title) AS "Title",
    CASE
        WHEN LOWER(TRIM(src.role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(src.role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(src.role__c)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(src.role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(src.preferred_language__c)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(src.preferred_language__c)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS src
