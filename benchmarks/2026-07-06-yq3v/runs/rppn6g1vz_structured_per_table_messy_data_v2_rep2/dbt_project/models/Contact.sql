-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    INITCAP(TRIM(contact.firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(contact.lastname)), '') AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    TRIM(contact.title) AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) IN ('decision maker', 'decision_maker', 'dm') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) IN ('end user', 'end_user', 'eu') THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) IN ('technical contact', 'technical_contact', 'tc') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) IN ('executive sponsor', 'executive_sponsor', 'es') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('de', 'german', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('fr', 'french', 'francais') THEN 'FR'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('es', 'spanish', 'espanol') THEN 'ES'
        WHEN LOWER(TRIM(contact.preferred_language__c)) IN ('it', 'italian', 'italiano') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(contact.accountid) AS "AccountId",
    TRIM(contact.id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact