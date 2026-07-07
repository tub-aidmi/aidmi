{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN LOWER(contact.role__c) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(contact.role__c) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        WHEN LOWER(contact.role__c) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(contact.role__c) IN ('end user', 'endanwender') THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(contact.preferred_language__c) IN ('deutsch', 'de') THEN 'DE'
        WHEN LOWER(contact.preferred_language__c) IN ('englisch', 'english', 'en') THEN 'EN'
        WHEN LOWER(contact.preferred_language__c) IN ('französisch', 'french', 'fr') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c", -- Source natural key
    '2023-01-01' AS "CreatedDate", -- Default value
    '2023-01-01' AS "LastModifiedDate", -- Default value
    0 AS "IsDeleted" -- Default value
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
