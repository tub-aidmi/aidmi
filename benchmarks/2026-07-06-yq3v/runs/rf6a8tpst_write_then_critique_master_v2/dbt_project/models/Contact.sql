{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), 'Unknown') AS "LastName",
    TRIM(LOWER(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakte.rolle) IN ('techniker', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('en', 'englisch') THEN 'EN'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('fr', 'französisch') THEN 'FR'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('es', 'spanisch') THEN 'ES'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('it', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON kontakte.kd_nummer = kunden.kundennummer
