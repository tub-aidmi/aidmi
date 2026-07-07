{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    TRIM(INITCAP(contact.firstname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(contact.lastname)), 'Unknown') AS "LastName",
    TRIM(LOWER(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    TRIM(INITCAP(contact.title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) IN ('decision maker', 'decisionmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) IN ('end user', 'enduser') THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) IN ('technical contact', 'technicalcontact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) IN ('executive sponsor', 'executivesponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('english', 'eng', 'en') THEN 'EN'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('german', 'deu', 'de') THEN 'DE'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('french', 'fra', 'fr') THEN 'FR'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('spanish', 'esp', 'es') THEN 'ES'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('italian', 'ita', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(contact.accountid) AS "AccountId",
    TRIM(contact.id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
