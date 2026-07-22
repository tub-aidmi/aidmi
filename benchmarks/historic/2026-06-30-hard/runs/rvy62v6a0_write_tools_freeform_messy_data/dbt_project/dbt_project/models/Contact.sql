{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    "FirstName" AS "FirstName",
    COALESCE("LastName", 'Unknown') AS "LastName", -- Target is NOT NULL
    "Email" AS "Email",
    "Phone" AS "Phone",
    "Title" AS "Title",
    CASE
        WHEN TRIM(LOWER(COALESCE("Role__c", ''))) IN ('decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(COALESCE("Role__c", ''))) IN ('end user') THEN 'End User'
        WHEN TRIM(LOWER(COALESCE("Role__c", ''))) IN ('technical contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(COALESCE("Role__c", ''))) IN ('executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(COALESCE("Preferred_Language__c", ''))) = 'DE' THEN 'DE'
        WHEN TRIM(UPPER(COALESCE("Preferred_Language__c", ''))) = 'EN' THEN 'EN'
        WHEN TRIM(UPPER(COALESCE("Preferred_Language__c", ''))) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(COALESCE("Preferred_Language__c", ''))) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(COALESCE("Preferred_Language__c", ''))) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "AccountId" AS "AccountId",
    NULL AS "Legacy_Contact_ID__c", -- Not in source, default to NULL
    NULL AS "CreatedDate", -- Not in source, default to NULL
    NULL AS "LastModifiedDate", -- Not in source, default to NULL
    0 AS "IsDeleted" -- Not in source, default to 0 (false)
FROM {{ source('fixture_messy_data_src', 'Contact') }}
