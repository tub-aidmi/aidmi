{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kontakt.kontakt_id)) AS "Id",
    TRIM(kontakt.rufname) AS "FirstName",
    COALESCE(TRIM(kontakt.familienname), 'Unknown') AS "LastName",
    TRIM(kontakt.kontakt_email) AS "Email",
    TRIM(kontakt.tel) AS "Phone",
    TRIM(kontakt.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(kontakt.rolle) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(kontakt.rolle) IN ('end user', 'end anwender', 'enduser') THEN 'End User'
        WHEN LOWER(kontakt.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(kontakt.rolle) IN ('sponsor', 'executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('de', 'german', 'deutsch') THEN 'DE'
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('fr', 'french', 'französisch') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(kunde.kundennummer)) AS "AccountId",
    kontakt.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakt
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunde
ON
    TRIM(kontakt.kd_nummer) = TRIM(kunde.kundennummer)