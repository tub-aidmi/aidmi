{{ config(materialized='table') }}

SELECT
    '003' || MD5(k.kontakt_id) AS "Id",
    TRIM(k.rufname) AS "FirstName",
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    REGEXP_REPLACE(TRIM(k.tel), '[^0-9+]', '', 'g') AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    CASE
        WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNIKER') THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LEFT(UPPER(TRIM(k.korrespondenzsprache)), 2) = 'DE' THEN 'DE'
        WHEN LEFT(UPPER(TRIM(k.korrespondenzsprache)), 2) = 'EN' OR UPPER(TRIM(k.korrespondenzsprache)) = 'GERMAN' THEN 'EN'
        WHEN LEFT(UPPER(TRIM(k.korrespondenzsprache)), 2) = 'FR' THEN 'FR'
        WHEN LEFT(UPPER(TRIM(k.korrespondenzsprache)), 2) = 'ES' THEN 'ES'
        WHEN LEFT(UPPER(TRIM(k.korrespondenzsprache)), 2) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    a.Id AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN (
    SELECT
        'A' || MD5(kundennummer) AS Id,
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
) a ON k.kd_nummer = a.kundennummer