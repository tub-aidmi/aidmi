{{ config(materialized='table') }}
SELECT
    c."id" AS "Id",
    TRIM(c."firstname") AS "FirstName",
    TRIM(c."lastname") AS "LastName",
    CASE
        WHEN c."email" IS NULL OR UPPER(TRIM(c."email")) = 'N/A' THEN NULL
        ELSE TRIM(c."email")
    END AS "Email",
    TRIM(c."phone") AS "Phone",
    TRIM(c."title") AS "Title",
    CASE
        WHEN UPPER(TRIM(c."role__c")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(c."role__c")) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(c."role__c")) IN ('TECHNICAL CONTACT', 'TECHNIKER', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(c."role__c")) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN c."role__c" IS NULL OR UPPER(TRIM(c."role__c")) IN ('', 'N/A') THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(c."preferred_language__c")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(c."preferred_language__c")) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(c."preferred_language__c")) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(c."preferred_language__c")) IN ('ES') THEN 'ES'
        WHEN UPPER(TRIM(c."preferred_language__c")) IN ('IT') THEN 'IT'
        WHEN c."preferred_language__c" IS NULL OR UPPER(TRIM(c."preferred_language__c")) IN ('', 'N/A') THEN NULL
        ELSE NULL
    END AS "Preferred_Language__c",
    a."id" AS "AccountId",
    c."id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN
    {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON c."accountid" = a."id"