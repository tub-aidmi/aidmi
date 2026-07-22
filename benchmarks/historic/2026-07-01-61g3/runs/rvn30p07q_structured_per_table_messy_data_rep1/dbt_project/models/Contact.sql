{{ config(materialized='table') }}

SELECT
    "Id",
    NULLIF(TRIM("FirstName"), '') AS "FirstName",
    COALESCE(NULLIF(TRIM("LastName"), ''), 'Unknown') AS "LastName",
    CASE WHEN TRIM("Email") IN ('', 'N/A') THEN NULL ELSE TRIM("Email") END AS "Email",
    CASE WHEN TRIM("Phone") IN ('', 'N/A') THEN NULL ELSE REGEXP_REPLACE(TRIM("Phone"), '[^0-9+]', '', 'g') END AS "Phone",
    NULLIF(TRIM("Title"), '') AS "Title",
    CASE
        WHEN TRIM(LOWER("Role__c")) = 'decision maker' THEN 'Decision Maker'
        WHEN TRIM(LOWER("Role__c")) = 'end user' THEN 'End User'
        WHEN TRIM(LOWER("Role__c")) = 'technical contact' THEN 'Technical Contact'
        WHEN TRIM(LOWER("Role__c")) = 'entscheider' THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM("Preferred_Language__c")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM("Preferred_Language__c")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'FR' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM("AccountId") AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Contact') }}