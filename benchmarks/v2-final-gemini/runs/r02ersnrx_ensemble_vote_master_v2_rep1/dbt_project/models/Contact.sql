{{ config(materialized='table') }}

SELECT
    TRIM(k.kontakt_id) AS "Id",
    TRIM(k.rufname) AS "FirstName",
    COALESCE(TRIM(k.familienname), 'Unknown') AS "LastName",
    TRIM(k.kontakt_email) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(k.rolle) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(k.rolle) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(k.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(k.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(k.korrespondenzsprache) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(k.korrespondenzsprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(k.korrespondenzsprache) IN ('fr', 'french') THEN 'FR'
        WHEN LOWER(k.korrespondenzsprache) IN ('es', 'spanish') THEN 'ES'
        WHEN LOWER(k.korrespondenzsprache) IN ('it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(m.kundennummer) AS "AccountId",
    TRIM(k.kontakt_id) AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS m
ON
    TRIM(k.kd_nummer) = TRIM(m.kundennummer)
