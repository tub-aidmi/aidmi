-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) IN ('decision maker', 'decisionmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) IN ('end user', 'enduser') THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) IN ('technical contact', 'technicalcontact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) IN ('executive sponsor', 'executivesponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('de', 'german') THEN 'DE'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('fr', 'french') THEN 'FR'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('es', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact