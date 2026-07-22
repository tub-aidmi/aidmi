{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), '') AS "LastName",
    TRIM(kontakte.kontakt_email) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE LOWER(TRIM(kontakte.rolle))
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'endbenutzer' THEN 'End User'
        WHEN 'technischer kontakt' THEN 'Technical Contact'
        WHEN 'geschaeftsfuehrer' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(kontakte.korrespondenzsprache))
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FRANZOESISCH' THEN 'FR'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    COALESCE(MD5(kunden.kundennummer), MD5('UNKNOWN_ACCOUNT')) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    kontakte.kd_nummer = kunden.kundennummer