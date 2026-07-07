-- This dbt model transforms raw contact data into the target Contact schema.
-- It handles type conversions, enum mapping, and provides default values for missing fields.

{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
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
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('fr', 'french', 'französisch') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact