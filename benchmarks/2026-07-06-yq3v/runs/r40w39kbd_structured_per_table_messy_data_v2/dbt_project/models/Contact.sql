{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) IN ('decision maker', 'decision_maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) IN ('end user', 'end_user') THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) IN ('technical contact', 'tech contact', 'technical_contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) IN ('executive sponsor', 'exec sponsor', 'executive_sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('DE', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('FR', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(contact.preferred_language__c)) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
