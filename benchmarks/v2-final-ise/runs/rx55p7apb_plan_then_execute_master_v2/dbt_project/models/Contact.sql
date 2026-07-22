{{ config(materialized='table') }}
SELECT
    k."kontakt_id" AS "Id",
    INITCAP(TRIM(k."rufname")) AS "FirstName",
    COALESCE(INITCAP(TRIM(k."familienname")), 'Unknown') AS "LastName",
    LOWER(TRIM(k."kontakt_email")) AS "Email",
    REGEXP_REPLACE(TRIM(k."tel"), '[^0-9+]', '', 'g') AS "Phone",
    INITCAP(TRIM(k."berufsbezeichnung")) AS "Title",
    CASE
        WHEN UPPER(TRIM(k."rolle")) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(k."rolle")) = 'ENDBENUTZER' THEN 'End User'
        WHEN UPPER(TRIM(k."rolle")) = 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(k."rolle")) = 'FÜHRUNGSKRAFT' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(k."korrespondenzsprache")) = 'DEUTSCH' THEN 'DE'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) = 'ENGLISCH' THEN 'EN'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) = 'FRANZÖSISCH' THEN 'FR'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) = 'SPANISCH' THEN 'ES'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) = 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c."kundennummer" AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    '2023-01-01'::text AS "CreatedDate",
    '2023-01-01'::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON TRIM(k."kd_nummer") = TRIM(c."kundennummer")