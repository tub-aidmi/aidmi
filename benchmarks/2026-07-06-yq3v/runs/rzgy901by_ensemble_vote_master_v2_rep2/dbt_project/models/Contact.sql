{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, 'Unknown') AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    kontakte.kd_nummer = kunden.kundennummer
