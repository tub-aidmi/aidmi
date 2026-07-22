{{ config(materialized='table') }}

SELECT
    mk."kontakt_id" AS "Id",
    TRIM(INITCAP(mk."rufname")) AS "FirstName",
    TRIM(INITCAP(mk."familienname")) AS "LastName",
    LOWER(TRIM(mk."kontakt_email")) AS "Email",
    TRIM(mk."tel") AS "Phone",
    TRIM(INITCAP(mk."berufsbezeichnung")) AS "Title",
    CASE
        WHEN TRIM(mk."rolle") = 'Entscheider' THEN 'Decision Maker'
        WHEN TRIM(mk."rolle") = 'Endnutzer' THEN 'End User'
        WHEN TRIM(mk."rolle") = 'Technischer Kontakt' THEN 'Technical Contact'
        WHEN TRIM(mk."rolle") = 'Führungskraft' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(mk."korrespondenzsprache") = 'Deutsch' THEN 'DE'
        WHEN TRIM(mk."korrespondenzsprache") = 'Englisch' THEN 'EN'
        WHEN TRIM(mk."korrespondenzsprache") = 'Französisch' THEN 'FR'
        WHEN TRIM(mk."korrespondenzsprache") = 'Spanisch' THEN 'ES'
        WHEN TRIM(mk."korrespondenzsprache") = 'Italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mk."kd_nummer" AS "AccountId",
    mk."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} mkunden ON TRIM(mk."kd_nummer") = TRIM(mkunden."kundennummer")