-- depends_on: {{ source('fixture_messy_data_src', 'Contact') }}
{{ config(materialized='table') }}

SELECT
    TRIM("Id") AS "Id",
    TRIM("FirstName") AS "FirstName",
    COALESCE(TRIM("LastName"), 'Unknown') AS "LastName",
    TRIM("Email") AS "Email",
    TRIM("Phone") AS "Phone",
    TRIM("Title") AS "Title",
    CASE
        WHEN UPPER(TRIM("Role__c")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM("Role__c")) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM("Role__c")) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM("Role__c")) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM("Preferred_Language__c")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM("Preferred_Language__c")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM("AccountId") AS "AccountId",
    CAST(NULL AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(NULL AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Contact') }}