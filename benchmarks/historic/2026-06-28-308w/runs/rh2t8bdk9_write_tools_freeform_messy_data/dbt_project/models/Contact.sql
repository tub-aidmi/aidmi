-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    "FirstName" AS "FirstName",
    COALESCE("LastName", 'Unknown') AS "LastName",
    "Email" AS "Email",
    "Phone" AS "Phone",
    "Title" AS "Title",
    CASE
        WHEN TRIM(LOWER("Role__c")) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN TRIM(LOWER("Role__c")) = 'end user' THEN 'End User'
        WHEN TRIM(LOWER("Role__c")) = 'technical contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER("Preferred_Language__c")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN TRIM(UPPER("Preferred_Language__c")) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "AccountId" AS "AccountId",
    NULL::text AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Contact') }}
