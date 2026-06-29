normalize.Case_Correction

{{ config(materialized='table') }}

SELECT
    src."Id" AS "Id",
    src."FirstName" AS "FirstName",
    COALESCE(TRIM(src."LastName"), '') AS "LastName",
    src."Email" AS "Email",
    src."Phone" AS "Phone",
    src."Title" AS "Title",
    CASE
        WHEN UPPER(TRIM(src."Role__c")) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(src."Role__c")) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(src."Role__c")) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(src."Role__c")) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(src."Role__c")) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(src."Preferred_Language__c")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(src."Preferred_Language__c")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(src."Preferred_Language__c")) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(src."Preferred_Language__c")) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(src."Preferred_Language__c")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    src."AccountId" AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Contact') }} AS src
