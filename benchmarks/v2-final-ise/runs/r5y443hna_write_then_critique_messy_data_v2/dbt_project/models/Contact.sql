{{ config(materialized='table') }}

SELECT
    CAST("id" AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE("firstname", ''))) AS "FirstName",
    COALESCE(INITCAP(TRIM("lastname")), 'Unknown') AS "LastName",
    LOWER(TRIM(COALESCE("email", ''))) AS "Email",
    TRIM(COALESCE("phone", '')) AS "Phone",
    INITCAP(TRIM(COALESCE("title", ''))) AS "Title",
    CASE
        WHEN UPPER(TRIM(COALESCE("role__c", ''))) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE("role__c", ''))) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(COALESCE("role__c", ''))) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(COALESCE("role__c", ''))) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(COALESCE("preferred_language__c", ''))) IN ('DE', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE("preferred_language__c", ''))) IN ('EN', 'ENGLISH', 'UK') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE("preferred_language__c", ''))) IN ('FR', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(COALESCE("preferred_language__c", ''))) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(COALESCE("preferred_language__c", ''))) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE 
        WHEN TRIM(COALESCE("accountid", '')) != '' THEN '001' || REGEXP_REPLACE(TRIM("accountid"), '[^0-9]', '', 'g')
        ELSE NULL
    END AS "AccountId",
    CAST("id" AS TEXT) AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}