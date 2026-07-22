{{ config(materialized='table') }}

SELECT
    k.kontakt_id AS "Id",
    k.rufname AS "FirstName",
    COALESCE(k.familienname, 'Unknown') AS "LastName", -- LastName is NOT NULL, providing a fallback
    k.kontakt_email AS "Email",
    k.tel AS "Phone",
    k.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(TRIM(k.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(k.rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(k.rolle)) IN ('techniker', 'technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(k.rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(kORRESPONDENZSPRACHE) = 'fr' OR LOWER(TRIM(k.korrespondenzsprache)) = 'französisch' THEN 'FR'
        -- Add ES, IT if source data appears
        ELSE NULL
    END AS "Preferred_Language__c",
    kd.kundennummer AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kd
    ON k.kd_nummer = kd.kundennummer
```