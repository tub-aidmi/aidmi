{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(lastname, 'Unknown') AS "LastName", -- LastName is NOT NULL, fallback to 'Unknown'
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE LOWER(TRIM(role__c))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'endanwender' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'techniker' THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        WHEN 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE LOWER(TRIM(preferred_language__c))
        WHEN 'de' THEN 'DE'
        WHEN 'deutsch' THEN 'DE'
        WHEN 'german' THEN 'DE'
        WHEN 'en' THEN 'EN'
        WHEN 'englisch' THEN 'EN'
        WHEN 'english' THEN 'EN'
        WHEN 'fr' THEN 'FR'
        WHEN 'französisch' THEN 'FR'
        WHEN 'french' THEN 'FR'
        WHEN 'es' THEN 'ES'
        WHEN 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c", -- Using id as the natural key for Legacy_Contact_ID__c
    NULL::TEXT AS "CreatedDate", -- Placeholder
    NULL::TEXT AS "LastModifiedDate", -- Placeholder
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
