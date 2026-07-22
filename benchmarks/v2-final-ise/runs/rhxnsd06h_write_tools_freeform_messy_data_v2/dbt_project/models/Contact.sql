{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    "firstname" AS "FirstName",
    COALESCE("lastname", 'Unknown') AS "LastName",
    NULLIF("email", 'N/A') AS "Email",
    "phone" AS "Phone",
    "title" AS "Title",
    CASE 
        WHEN UPPER(TRIM("role__c")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM("role__c")) IN ('END USER', 'ENDANWENDER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM("role__c")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNICKER') THEN 'Technical Contact'
        WHEN UPPER(TRIM("role__c")) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM("preferred_language__c")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('FR', 'FRENCH', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "accountid" AS "AccountId",
    "id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
