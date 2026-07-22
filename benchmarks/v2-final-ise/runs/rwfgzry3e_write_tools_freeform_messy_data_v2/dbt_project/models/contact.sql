{{ config(materialized='table') }}

SELECT
    CAST(id AS text) AS "Id",
    COALESCE(INITCAP(TRIM(firstname)), 'Unknown') AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    TRIM(email) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    INITCAP(TRIM(role__c)) AS "Role__c",
    INITCAP(TRIM(preferred_language__c)) AS "PreferredLanguage__c",
    accountid AS "AccountId",
    CAST(id AS text) AS "Legacy_Contact_ID__c",
    NULL::DATE AS "CreatedDate",
    NULL::DATE AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
