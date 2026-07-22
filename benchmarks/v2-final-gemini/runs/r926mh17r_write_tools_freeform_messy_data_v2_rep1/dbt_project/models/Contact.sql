{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(lastname, 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
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
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
