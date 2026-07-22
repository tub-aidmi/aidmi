{{ config(materialized='table') }}

WITH contact_enum_mappings AS (
    SELECT
        kontakt_id,
        -- Role__c enum mapping
        CASE
            WHEN LOWER(TRIM(rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
            WHEN LOWER(TRIM(rolle)) IN ('end user', 'endanwender', 'enduser') THEN 'End User'
            WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
            WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
            ELSE NULL
        END AS mapped_role,
        -- Preferred_Language__c enum mapping
        CASE
            WHEN LOWER(TRIM(korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
            WHEN LOWER(TRIM(korrespondenzsprache)) IN ('en', 'englisch', 'english') THEN 'EN'
            WHEN LOWER(TRIM(korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
            WHEN LOWER(TRIM(korrespondenzsprache)) IN ('es') THEN 'ES'
            WHEN LOWER(TRIM(korrespondenzsprache)) IN ('it') THEN 'IT'
            ELSE NULL
        END AS mapped_language
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
)

SELECT
    k.kontakt_id AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    INITCAP(TRIM(k.familienname)) AS "LastName",
    k.kontakt_email AS "Email",
    k.tel AS "Phone",
    k.berufsbezeichnung AS "Title",
    m.mapped_role AS "Role__c",
    m.mapped_language AS "Preferred_Language__c",
    c.kundennummer AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN contact_enum_mappings m ON k.kontakt_id = m.kontakt_id
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c ON k.kd_nummer = c.kundennummer