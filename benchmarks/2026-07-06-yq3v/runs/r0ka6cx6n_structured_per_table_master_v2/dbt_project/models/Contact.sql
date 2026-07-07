-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    COALESCE(mk.familienname, 'Contact ' || mk.kontakt_id) AS "LastName",
    mk.kontakt_email AS "Email",
    mk.tel AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE
        WHEN mk.rolle ILIKE '%entscheider%' THEN 'Decision Maker'
        WHEN mk.rolle ILIKE '%endbenutzer%' THEN 'End User'
        WHEN mk.rolle ILIKE '%technisch%' THEN 'Technical Contact'
        WHEN mk.rolle ILIKE '%geschäftsführer%' OR mk.rolle ILIKE '%vorstand%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN mk.korrespondenzsprache ILIKE '%deutsch%' OR mk.korrespondenzsprache ILIKE '%de%' THEN 'DE'
        WHEN mk.korrespondenzsprache ILIKE '%englisch%' OR mk.korrespondenzsprache ILIKE '%en%' THEN 'EN'
        WHEN mk.korrespondenzsprache ILIKE '%französisch%' OR mk.korrespondenzsprache ILIKE '%fr%' THEN 'FR'
        WHEN mk.korrespondenzsprache ILIKE '%spanisch%' OR mk.korrespondenzsprache ILIKE '%es%' THEN 'ES'
        WHEN mk.korrespondenzsprache ILIKE '%italienisch%' OR mk.korrespondenzsprache ILIKE '%it%' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mck.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    NULL AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mck
ON
    mk.kd_nummer = mck.kundennummer;