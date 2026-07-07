{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN UPPER(TRIM(contact.role__c)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(contact.role__c)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(contact.role__c)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(contact.role__c)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
