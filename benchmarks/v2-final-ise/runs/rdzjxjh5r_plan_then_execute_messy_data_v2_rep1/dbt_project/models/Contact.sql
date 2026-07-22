{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(c.lastname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(c.title) AS "Title",
    CASE
        WHEN TRIM(LOWER(c.role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN TRIM(LOWER(c.role__c)) IN ('end user') THEN 'End User'
        WHEN TRIM(LOWER(c.role__c)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN TRIM(LOWER(c.role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('deutsch', 'de') THEN 'DE'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('englisch', 'en') THEN 'EN'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('französisch', 'french', 'fr') THEN 'FR'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('es') THEN 'ES'
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON c.accountid = a.id