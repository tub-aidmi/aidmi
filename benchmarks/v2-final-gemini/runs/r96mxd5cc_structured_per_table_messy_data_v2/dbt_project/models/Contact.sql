-- config
{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    TRIM(INITCAP(contact.firstname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(contact.lastname)), 'Unknown') AS "LastName",
    TRIM(LOWER(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    TRIM(contact.title) AS "Title",
    CASE
        WHEN TRIM(LOWER(contact.role__c)) IN ('decision maker', 'decision_maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(contact.role__c)) IN ('end user', 'end_user') THEN 'End User'
        WHEN TRIM(LOWER(contact.role__c)) IN ('technical contact', 'technical') THEN 'Technical Contact'
        WHEN TRIM(LOWER(contact.role__c)) IN ('executive sponsor', 'exec sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(LOWER(contact.preferred_language__c)) IN ('de', 'german', 'deutsch') THEN 'DE'
        WHEN TRIM(LOWER(contact.preferred_language__c)) IN ('en', 'english') THEN 'EN'
        WHEN TRIM(LOWER(contact.preferred_language__c)) IN ('fr', 'french', 'français') THEN 'FR'
        WHEN TRIM(LOWER(contact.preferred_language__c)) IN ('es', 'spanish', 'español') THEN 'ES'
        WHEN TRIM(LOWER(contact.preferred_language__c)) IN ('it', 'italian', 'italiano') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact