{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN TRIM(UPPER(contact.role__c)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN TRIM(UPPER(contact.role__c)) = 'END USER' THEN 'End User'
        WHEN TRIM(UPPER(contact.role__c)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN TRIM(UPPER(contact.role__c)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
