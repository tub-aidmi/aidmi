{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    TRIM(COALESCE(mk.familienname, '')) AS "LastName",
    TRIM(mk.kontakt_email) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(mk.rolle) IN ('decision maker', 'entscheidungsträger') THEN 'Decision Maker'
        WHEN LOWER(mk.rolle) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(mk.rolle) IN ('technical contact', 'technischer kontakt') THEN 'Technical Contact'
        WHEN LOWER(mk.rolle) IN ('executive sponsor', 'führungskraft', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(mk.korrespondenzsprache) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(mk.korrespondenzsprache) IN ('en', 'englisch') THEN 'EN'
        WHEN LOWER(mk.korrespondenzsprache) IN ('fr', 'französisch') THEN 'FR'
        WHEN LOWER(mk.korrespondenzsprache) IN ('es', 'spanisch') THEN 'ES'
        WHEN LOWER(mk.korrespondenzsprache) IN ('it', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mc.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mc
    ON mk.kd_nummer = mc.kundennummer
