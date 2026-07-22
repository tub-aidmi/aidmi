{{
    config(materialized='table')
}}

SELECT
    "Id" AS "Id",
    "FirstName" AS "FirstName",
    COALESCE("LastName", '') AS "LastName",
    "Email" AS "Email",
    "Phone" AS "Phone",
    "Title" AS "Title",
    CASE
        WHEN TRIM(UPPER("Role__c")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN TRIM(UPPER("Role__c")) = 'END USER' THEN 'End User'
        WHEN TRIM(UPPER("Role__c")) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN TRIM(UPPER("Role__c")) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER("Preferred_Language__c")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN TRIM(UPPER("Preferred_Language__c")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER("Preferred_Language__c")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "AccountId" AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Contact') }}