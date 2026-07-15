{{ config(materialized='table') }}

SELECT
    CAST("id" AS TEXT) AS "Id",
    INITCAP(TRIM("firstname")) AS "FirstName",
    COALESCE(NULLIF(TRIM("lastname"), ''), '') AS "LastName",
    LOWER(TRIM("email")) AS "Email",
    TRIM("phone") AS "Phone",
    INITCAP(TRIM("title")) AS "Title",

    CASE UPPER(TRIM("role__c"))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER'       THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    CASE UPPER(TRIM("preferred_language__c"))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    COALESCE(acc."id", TRIM(c."accountid")) AS "AccountId",

    CAST("id" AS TEXT) AS "Legacy_Contact_ID__c",

    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0          AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acc 
    ON TRIM(c."accountid") = TRIM(acc."erp_number__c")