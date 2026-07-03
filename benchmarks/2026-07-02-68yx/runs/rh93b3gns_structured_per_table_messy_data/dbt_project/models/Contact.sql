{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    NULLIF(TRIM("FirstName"), '') AS "FirstName",
    CASE
        WHEN TRIM("LastName") IS NULL OR TRIM("LastName") = '' THEN 'Unknown'
        ELSE INITCAP(TRIM("LastName"))
    END AS "LastName",
    CASE WHEN UPPER(TRIM(COALESCE("Email", ''))) = 'N/A' THEN NULL ELSE TRIM(COALESCE("Email", '')) END AS "Email",
    CASE WHEN TRIM(COALESCE("Phone", '')) IN ('', 'N/A') THEN NULL ELSE TRIM(COALESCE("Phone", '')) END AS "Phone",
    NULLIF(TRIM("Title"), '') AS "Title",
    CASE
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) IN ('entscheider', 'executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('fr', 'français', 'french') THEN 'FR'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('es', 'español', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('it', 'italiano', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "AccountId" AS "AccountId",
    CAST(NULL AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Contact') }}