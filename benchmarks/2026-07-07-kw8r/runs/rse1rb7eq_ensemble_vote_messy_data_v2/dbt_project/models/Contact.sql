{{ config(materialized='table') }}

SELECT
    'SFDC-' || LPAD(SUBSTRING(c.id FROM '\d+'), 8, '0') AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(c.lastname)), 'Unknown') AS "LastName",
    CASE
        WHEN c.email IS NULL OR TRIM(c.email) = '' OR TRIM(UPPER(c.email)) = 'N/A' THEN NULL
        ELSE UPPER(TRIM(c.email))
    END AS "Email",
    TRIM(c.phone) AS "Phone",
    INITCAP(TRIM(c.title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(COALESCE(c.role__c, ''))) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE(c.role__c, ''))) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(COALESCE(c.role__c, ''))) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(COALESCE(c.role__c, ''))) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('DE', 'DEUTSCH', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('EN', 'ENGLISCH', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('FR', 'FRANZÖSCH', 'FRANZÖSISCH', 'FRENCH', 'FRENCH') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN c.accountid IS NOT NULL AND SUBSTRING(c.accountid FROM '\d+') IS NOT NULL
            THEN 'SFDC-' || LPAD(SUBSTRING(c.accountid FROM '\d+'), 8, '0')
        ELSE NULL
    END AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c