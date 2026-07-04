{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    TRIM(contact.firstname) AS "FirstName",
    COALESCE(TRIM(contact.lastname), 'Unknown') AS "LastName",
    TRIM(contact.email) AS "Email",
    TRIM(contact.phone) AS "Phone",
    TRIM(contact.title) AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(contact.preferred_language__c)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(contact.preferred_language__c)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(contact.accountid) AS "AccountId",
    TRIM(contact.id) AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
WHERE
    contact.id IS NOT NULL
