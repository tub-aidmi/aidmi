{{ config(materialized='table') }}

SELECT
    id AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    NULLIF(TRIM(email), '') AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN UPPER(TRIM(role__c)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(role__c)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(role__c)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNIKER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(role__c)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN role__c IS NULL OR TRIM(role__c) = '' OR UPPER(TRIM(role__c)) = 'N/A' THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
