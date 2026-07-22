{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    TRIM(INITCAP(contact.firstname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(contact.lastname)), 'Unknown') AS "LastName",
    TRIM(LOWER(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    TRIM(contact.title) AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) IN ('decision maker', 'dm') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) IN ('end user', 'eu') THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) IN ('technical contact', 'tech contact', 'tc') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) IN ('executive sponsor', 'exec sponsor', 'es') THEN 'Executive Sponsor'
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
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
