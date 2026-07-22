{{ config(materialized='table') }}

SELECT
    kontakt.kontakt_id AS "Id",
    kontakt.rufname AS "FirstName",
    COALESCE(kontakt.familienname, 'Unknown') AS "LastName",
    kontakt.kontakt_email AS "Email",
    kontakt.tel AS "Phone",
    kontakt.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakt.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(kontakt.rolle) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(kontakt.rolle) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(kontakt.rolle) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakt.kd_nummer AS "AccountId",
    kontakt.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakt
