{{ config(materialized='table') }}

SELECT
    k."kontakt_id" AS "Id",
    TRIM(k."rufname") AS "FirstName",
    TRIM(k."familienname") AS "LastName",
    TRIM(LOWER(k."kontakt_email")) AS "Email",
    REGEXP_REPLACE(k."tel", '[^0-9+]', '', 'g') AS "Phone",
    INITCAP(TRIM(k."berufsbezeichnung")) AS "Title",
    CASE
        WHEN TRIM(LOWER(k."rolle")) = 'entscheider' THEN 'Decision Maker'
        WHEN TRIM(LOWER(k."rolle")) = 'endbenutzer' THEN 'End User'
        WHEN TRIM(LOWER(k."rolle")) IN ('technischer kontakt', 'technischer_kontakt') THEN 'Technical Contact'
        WHEN TRIM(LOWER(k."rolle")) = 'führungskraft' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(LOWER(k."korrespondenzsprache")) IN ('deutsch', 'de') THEN 'DE'
        WHEN TRIM(LOWER(k."korrespondenzsprache")) IN ('englisch', 'en') THEN 'EN'
        WHEN TRIM(LOWER(k."korrespondenzsprache")) IN ('französisch', 'französisch', 'fr') THEN 'FR'
        WHEN TRIM(LOWER(k."korrespondenzsprache")) IN ('spanisch', 'es') THEN 'ES'
        WHEN TRIM(LOWER(k."korrespondenzsprache")) IN ('italienisch', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c."kundennummer" AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON TRIM(k."kd_nummer") = TRIM(c."kundennummer")