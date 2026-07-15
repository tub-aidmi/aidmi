{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        firstname,
        lastname,
        email,
        phone,
        title,
        role__c,
        preferred_language__c,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
),

transformed AS (
    SELECT
        id AS "Id",
        TRIM(INITCAP(firstname)) AS "FirstName",
        COALESCE(TRIM(lastname), '') AS "LastName",
        TRIM(LOWER(email)) AS "Email",
        TRIM(phone) AS "Phone",
        TRIM(INITCAP(title)) AS "Title",
        CASE
            WHEN LOWER(TRIM(role__c)) IN ('decision maker') THEN 'Decision Maker'
            WHEN LOWER(TRIM(role__c)) IN ('end user') THEN 'End User'
            WHEN LOWER(TRIM(role__c)) IN ('technical contact') THEN 'Technical Contact'
            WHEN LOWER(TRIM(role__c)) IN ('executive sponsor') THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",
        CASE
            WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'GERMAN') THEN 'DE'
            WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENGLISH') THEN 'EN'
            WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRENCH') THEN 'FR'
            WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'SPANISH') THEN 'ES'
            WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITALIAN') THEN 'IT'
            ELSE NULL
        END AS "Preferred_Language__c",
        CAST(accountid AS TEXT) AS "AccountId",
        id AS "Legacy_Contact_ID__c",
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM source_data
)

SELECT * FROM transformed
