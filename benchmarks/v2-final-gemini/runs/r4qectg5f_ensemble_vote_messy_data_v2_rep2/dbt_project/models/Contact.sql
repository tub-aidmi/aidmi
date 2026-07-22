{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    TRIM(contact.firstname) AS "FirstName",
    COALESCE(TRIM(contact.lastname), '') AS "LastName",
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
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('es', 'spanish') THEN 'ES' -- Assuming Spanish might exist even if not in sample
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('it', 'italienisch', 'italian') THEN 'IT' -- Assuming Italian might exist even if not in sample
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(contact.accountid) AS "AccountId",
    TRIM(contact.id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
