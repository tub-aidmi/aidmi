{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    COALESCE(mk.familienname, 'Unknown') AS "LastName",
    mk.kontakt_email AS "Email",
    mk.tel AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(mk.rolle) IN ('sponsor', 'executive sponsor') THEN 'Executive Sponsor'
        WHEN LOWER(mk.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(mk.rolle) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(mk.rolle) IN ('techniker', 'technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(mk.korrespondenzsprache) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN LOWER(mk.korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(mk.korrespondenzsprache) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    mck.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mck
ON
    mk.kd_nummer = mck.kundennummer
