
{{ config(materialized='table') }}

SELECT
    -- Id: Unique identifier for the contact.
    "Id" AS "Id",

    -- FirstName: First name of the contact.
    "FirstName" AS "FirstName",

    -- LastName: Last name of the contact. Provides 'Unknown' if source is NULL to satisfy NOT NULL constraint.
    COALESCE("LastName", 'Unknown') AS "LastName",

    -- Email: Email address of the contact.
    "Email" AS "Email",

    -- Phone: Phone number of the contact.
    "Phone" AS "Phone",

    -- Title: Job title of the contact.
    "Title" AS "Title",

    -- Role__c: Maps various source role descriptions to a standardized enum list.
    CASE
        WHEN TRIM("Role__c") ILIKE 'Decision Maker' THEN 'Decision Maker'
        WHEN TRIM("Role__c") ILIKE 'Entscheider' THEN 'Decision Maker'
        WHEN TRIM("Role__c") ILIKE 'End User' THEN 'End User'
        WHEN TRIM("Role__c") ILIKE 'Technical Contact' THEN 'Technical Contact'
        WHEN TRIM("Role__c") ILIKE 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: Maps source language codes/names to ISO 639-1 two-letter codes.
    CASE
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'DEUTSCH' THEN 'DE'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'ENGLISH' THEN 'EN'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'ENGLISCH' THEN 'EN'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM("Preferred_Language__c")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: Identifier for the associated account.
    "AccountId" AS "AccountId",

    -- Legacy_Contact_ID__c: No direct source, defaulting to NULL.
    NULL AS "Legacy_Contact_ID__c",

    -- CreatedDate: No direct source, defaulting to NULL.
    NULL AS "CreatedDate",

    -- LastModifiedDate: No direct source, defaulting to NULL.
    NULL AS "LastModifiedDate",

    -- IsDeleted: Defaulting to 0 as no source column indicates deletion status.
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Contact') }}
