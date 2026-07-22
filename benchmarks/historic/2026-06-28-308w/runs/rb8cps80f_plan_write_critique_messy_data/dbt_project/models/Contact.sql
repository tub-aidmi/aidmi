
{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    TRIM("FirstName") AS "FirstName",
    COALESCE(TRIM("LastName"), 'Unknown') AS "LastName",
    LOWER(TRIM("Email")) AS "Email",
    TRIM("Phone") AS "Phone",
    INITCAP(TRIM("Title")) AS "Title",
    CASE UPPER(TRIM("Role__c"))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM("Preferred_Language__c"))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    "AccountId" AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Contact') }}
