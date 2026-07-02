{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE("FirstName", ''))) AS "FirstName",
    CASE
        WHEN TRIM(COALESCE("LastName", '')) = '' THEN 'Unknown'
        ELSE INITCAP(TRIM("LastName"))
    END AS "LastName",
    CASE
        WHEN LOWER(TRIM(COALESCE("Email", ''))) IN ('n/a', '') THEN NULL
        ELSE LOWER(TRIM("Email"))
    END AS "Email",
    CASE
        WHEN TRIM(COALESCE("Phone", '')) = 'N/A' OR TRIM(COALESCE("Phone", '')) = '' THEN NULL
        ELSE "Phone"
    END AS "Phone",
    INITCAP(TRIM("Title")) AS "Title",
    CASE
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) IN ('end user') THEN 'End User'
        WHEN LOWER(TRIM(COALESCE("Role__c", ''))) IN ('technical contact') THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('de', 'deutsch', 'german', 'germany') THEN 'DE'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('en', 'english', 'englisch', 'american', 'british') THEN 'EN'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST("AccountId" AS TEXT) AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Contact') }}