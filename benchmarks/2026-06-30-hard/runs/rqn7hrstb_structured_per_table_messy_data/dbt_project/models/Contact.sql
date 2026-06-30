-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    Id AS "Id",
    FirstName AS "FirstName",
    COALESCE(LastName, 'Unknown') AS "LastName",
    Email AS "Email",
    Phone AS "Phone",
    Title AS "Title",
    CASE
        WHEN UPPER(TRIM("Role__c")) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM("Role__c")) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM("Role__c")) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM("Role__c")) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    AccountId AS "AccountId",
    Id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Contact') }}