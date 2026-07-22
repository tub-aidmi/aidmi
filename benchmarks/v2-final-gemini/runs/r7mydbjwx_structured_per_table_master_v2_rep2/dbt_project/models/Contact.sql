-- noinspection SqlNoDataSourceInspection
-- noinspection SqlDialectInspection

{{ config(materialized='table') }}

SELECT
    MD5(kont.kontakt_id) AS "Id",
    TRIM(INITCAP(kont.rufname)) AS "FirstName",
    TRIM(INITCAP(kont.familienname)) AS "LastName",
    TRIM(LOWER(kont.kontakt_email)) AS "Email",
    TRIM(kont.tel) AS "Phone",
    TRIM(INITCAP(kont.berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(kont.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(kont.rolle) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(kont.rolle) IN ('technical contact', 'technischer kontakt') THEN 'Technical Contact'
        WHEN LOWER(kont.rolle) IN ('executive sponsor', 'geschäftsführer') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kont.korrespondenzsprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(kont.korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(kont.korrespondenzsprache) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(kont.korrespondenzsprache) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(kont.korrespondenzsprache) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(kont.kd_nummer)) AS "AccountId",
    kont.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kont