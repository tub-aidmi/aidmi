{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    TRIM("firstname") AS "FirstName",
    COALESCE(NULLIF(TRIM("lastname"), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM("email"), '') AS "Email",
    NULLIF(TRIM("phone"), '') AS "Phone",
    TRIM("title") AS "Title",
    CASE
        WHEN UPPER(TRIM("role__c")) IN ('DECISION MAKER', 'ENTSCHEIDER', 'ENTSCHEIDERIN') THEN 'Decision Maker'
        WHEN UPPER(TRIM("role__c")) IN ('END USER', 'ENDANWENDER', 'ENDANWENDERIN') THEN 'End User'
        WHEN UPPER(TRIM("role__c")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHE ANSPRECHPARTNERIN', 'TECHNIKER', 'TECHNIKERIN') THEN 'Technical Contact'
        WHEN UPPER(TRIM("role__c")) IN ('EXECUTIVE SPONSOR', 'SPONSOR', 'SPONSORIN') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM("preferred_language__c")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('FR', 'FRENCH', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM("preferred_language__c")) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "accountid" AS "AccountId",
    "id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
