{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(lastname, 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN LOWER(role__c) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(role__c) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(role__c) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(role__c) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(preferred_language__c) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(preferred_language__c) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(preferred_language__c) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(preferred_language__c) = 'es' THEN 'ES'
        WHEN LOWER(preferred_language__c) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}