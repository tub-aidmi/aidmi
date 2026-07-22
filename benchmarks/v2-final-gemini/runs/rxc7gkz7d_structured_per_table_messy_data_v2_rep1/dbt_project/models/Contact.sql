{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN LOWER(contact.role__c) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(contact.role__c) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(contact.role__c) IN ('technical contact', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(contact.role__c) IN ('sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(contact.preferred_language__c) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(contact.preferred_language__c) IN ('en', 'englisch') THEN 'EN'
        WHEN LOWER(contact.preferred_language__c) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
