{{ config(materialized='table') }}

SELECT
    c."id" AS "Id",
    INITCAP(TRIM(c."firstname")) AS "FirstName",
    INITCAP(TRIM(c."lastname")) AS "LastName",
    LOWER(TRIM(c."email")) AS "Email",
    TRIM(c."phone") AS "Phone",
    INITCAP(TRIM(c."title")) AS "Title",
    CASE 
        WHEN LOWER(TRIM(c."role__c")) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(c."role__c")) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(c."role__c")) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(c."role__c")) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    UPPER(TRIM(c."preferred_language__c")) AS "Preferred_Language__c",
    a."id" AS "AccountId",
    c."id" AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON c."accountid" = a."id"