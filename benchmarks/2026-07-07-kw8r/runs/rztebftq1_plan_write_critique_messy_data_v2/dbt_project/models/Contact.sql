{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'end user', 'technical contact', 'executive sponsor')
        THEN INITCAP(TRIM(role__c))
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT')
        THEN TRIM(UPPER(preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}