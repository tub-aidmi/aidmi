{{ config(materialized='table') }}

SELECT
    id AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown Last Name') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN UPPER(TRIM(role__c)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(role__c)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(role__c)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(role__c)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
