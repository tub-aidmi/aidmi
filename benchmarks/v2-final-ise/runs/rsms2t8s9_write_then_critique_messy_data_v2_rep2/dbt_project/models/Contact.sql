{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN COALESCE(LOWER(TRIM(role__c)), '') = 'decision maker' THEN 'Decision Maker'
        WHEN COALESCE(LOWER(TRIM(role__c)), '') = 'end user' THEN 'End User'
        WHEN COALESCE(LOWER(TRIM(role__c)), '') = 'technical contact' THEN 'Technical Contact'
        WHEN COALESCE(LOWER(TRIM(role__c)), '') = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN COALESCE(UPPER(TRIM(preferred_language__c)), '') IN ('DE', 'EN', 'FR', 'ES', 'IT')
            THEN UPPER(TRIM(preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}