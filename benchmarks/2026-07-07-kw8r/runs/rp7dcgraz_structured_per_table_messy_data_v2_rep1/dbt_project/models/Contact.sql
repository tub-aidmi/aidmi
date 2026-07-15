{{ config(materialized='table') }}

SELECT 
    c."id" AS "Id",
    TRIM(INITCAP(c."firstname")) AS "FirstName",
    TRIM(INITCAP(c."lastname")) AS "LastName",
    NULLIF(TRIM(c."email"), 'N/A') AS "Email",
    TRIM(c."phone") AS "Phone",
    TRIM(INITCAP(c."title")) AS "Title",
    CASE 
        WHEN TRIM(LOWER(c."role__c")) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN TRIM(LOWER(c."role__c")) IN ('end user', 'endanwender', 'enduser') THEN 'End User'
        WHEN TRIM(LOWER(c."role__c")) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN TRIM(LOWER(c."role__c")) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(c."preferred_language__c")) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN TRIM(LOWER(c."preferred_language__c")) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN TRIM(LOWER(c."preferred_language__c")) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN TRIM(LOWER(c."preferred_language__c")) IN ('es', 'spanisch') THEN 'ES'
        WHEN TRIM(LOWER(c."preferred_language__c")) IN ('it', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c."accountid" AS "AccountId",
    c."id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c