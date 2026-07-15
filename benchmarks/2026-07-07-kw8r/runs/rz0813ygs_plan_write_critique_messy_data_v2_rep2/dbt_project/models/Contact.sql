{{ config(materialized='table') }}

SELECT
    CAST(c.id AS TEXT) AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    INITCAP(TRIM(COALESCE(NULLIF(c.lastname, ''), 'Unknown'))) AS "LastName",
    CASE WHEN LOWER(TRIM(c.email)) IN ('n/a', '', null) THEN NULL ELSE LOWER(TRIM(c.email)) END AS "Email",
    TRIM(c.phone) AS "Phone",
    INITCAP(TRIM(c.title)) AS "Title",
    CASE UPPER(TRIM(COALESCE(c.role__c, '')))
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'END USER' THEN 'End User'
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(c.preferred_language__c, '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'SPANISH' THEN 'ES'
        WHEN 'SPANSICH' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        WHEN 'ITALIAN' THEN 'IT'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(a.id) AS "AccountId",
    TRIM(c.id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON TRIM(a.id) = TRIM(c.accountid)