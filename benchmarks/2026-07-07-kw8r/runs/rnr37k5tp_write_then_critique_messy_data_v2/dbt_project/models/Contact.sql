{{ config(materialized='table') }}

SELECT
    -- Id: source contact id (NOT NULL)
    CAST(c.id AS TEXT) AS "Id",

    -- FirstName
    INITCAP(TRIM(c.firstname)) AS "FirstName",

    -- LastName: NOT NULL - fallback to 'Unknown' if null/empty
    CASE
        WHEN TRIM(COALESCE(c.lastname, '')) = '' THEN 'Unknown'
        ELSE INITCAP(TRIM(c.lastname))
    END AS "LastName",

    -- Email: replace NULL, empty strings, and 'N/A' (any case) with NULL
    CASE
        WHEN TRIM(COALESCE(c.email, '')) IN ('', 'N/A', 'n/a') THEN NULL
        ELSE TRIM(LOWER(c.email))
    END AS "Email",

    -- Phone
    TRIM(c.phone) AS "Phone",

    -- Title
    INITCAP(TRIM(c.title)) AS "Title",

    -- Role__c: map various German/English terms to enum values
    CASE
        WHEN UPPER(TRIM(COALESCE(c.role__c, ''))) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(COALESCE(c.role__c, ''))) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(COALESCE(c.role__c, ''))) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(c.role__c, ''))) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: map language names and codes to ISO codes
    CASE
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('DEUTSCH', 'GERMAN', 'DE') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('ENGLISCH', 'ENGLISH', 'EN') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('FRANZÖSISCH', 'FRENCH', 'FR') THEN 'FR'
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('ITALIENISCH', 'ITALIAN', 'IT') THEN 'IT'
        WHEN UPPER(TRIM(COALESCE(c.preferred_language__c, ''))) IN ('SPANISCH', 'SPANISH', 'ES') THEN 'ES'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: normalized key matching Account.Id format (trimmed only, no LOWER)
    TRIM(c.accountid) AS "AccountId",

    -- Legacy_Contact_ID__c: natural key for verification
    c.id AS "Legacy_Contact_ID__c",

    -- CreatedDate: not available in source
    NULL::TEXT AS "CreatedDate",

    -- LastModifiedDate: not available in source
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: default 0
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c