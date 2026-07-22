{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    src.firstname AS "FirstName",
    COALESCE(TRIM(src.lastname), 'Unknown') AS "LastName",
    src.email AS "Email",
    src.phone AS "Phone",
    src.title AS "Title",
    CASE
        WHEN LOWER(TRIM(src.role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(src.role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(src.role__c)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(src.role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('es', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(src.preferred_language__c)) IN ('it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS src
