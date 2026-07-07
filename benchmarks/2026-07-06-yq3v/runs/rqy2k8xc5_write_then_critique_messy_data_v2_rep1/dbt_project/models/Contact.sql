{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(lastname, '') AS "LastName", -- LastName is NOT NULL, so provide a default empty string if source is NULL
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
        WHEN LOWER(preferred_language__c) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(preferred_language__c) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c", -- Using source contact id as the legacy ID
    NULL::text AS "CreatedDate", -- No source for CreatedDate
    NULL::text AS "LastModifiedDate", -- No source for LastModifiedDate
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
