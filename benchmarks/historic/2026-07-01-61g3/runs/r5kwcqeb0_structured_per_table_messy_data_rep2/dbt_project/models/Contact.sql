{{ config(materialized='table') }}

SELECT
    "Id"::text AS "Id",
    TRIM("FirstName") AS "FirstName",
    CASE WHEN TRIM(Lower("LastName")) = '' OR "LastName" IS NULL THEN 'Unknown' ELSE TRIM("LastName") END AS "LastName",
    TRIM(LOWER("Email")) AS "Email",
    REGEXP_REPLACE(TRIM("Phone"), '[^\d\+]', '', 'g') AS "Phone",
    INITCAP(TRIM("Title")) AS "Title",
    CASE
        WHEN Upper(TRIM(COALESCE("Role__c", ''))) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN Upper(TRIM(COALESCE("Role__c", ''))) IN ('END USER') THEN 'End User'
        WHEN Upper(TRIM(COALESCE("Role__c", ''))) IN ('TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN Upper(TRIM(COALESCE("Role__c", ''))) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN Upper(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN Upper(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN Upper(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN Upper(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN Upper(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM("AccountId") AS "AccountId",
    NULL::text AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Contact') }}