{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(lastname, 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN LOWER(role__c) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(role__c) = 'end user' THEN 'End User'
        WHEN LOWER(role__c) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(role__c) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(preferred_language__c) = 'DE' THEN 'DE'
        WHEN UPPER(preferred_language__c) = 'EN' THEN 'EN'
        WHEN UPPER(preferred_language__c) = 'FR' THEN 'FR'
        WHEN UPPER(preferred_language__c) = 'ES' THEN 'ES'
        WHEN UPPER(preferred_language__c) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c", -- Using source ID as legacy ID
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}