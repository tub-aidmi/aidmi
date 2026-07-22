{{ config(materialized='table') }}

SELECT
    MD5(kont.kontakt_id) AS "Id",
    TRIM(kont.rufname) AS "FirstName",
    COALESCE(TRIM(kont.familienname), '') AS "LastName",
    TRIM(kont.kontakt_email) AS "Email",
    TRIM(kont.tel) AS "Phone",
    TRIM(kont.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(kont.rolle) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(kont.rolle) LIKE '%nutzer%' THEN 'End User'
        WHEN LOWER(kont.rolle) LIKE '%technisch%' THEN 'Technical Contact'
        WHEN LOWER(kont.rolle) LIKE '%leiter%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kont.korrespondenzsprache) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(kont.korrespondenzsprache) IN ('en', 'englisch') THEN 'EN'
        WHEN LOWER(kont.korrespondenzsprache) IN ('fr', 'franzoesisch') THEN 'FR'
        WHEN LOWER(kont.korrespondenzsprache) IN ('es', 'spanisch') THEN 'ES'
        WHEN LOWER(kont.korrespondenzsprache) IN ('it', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kont.kd_nummer) AS "AccountId",
    kont.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kont
