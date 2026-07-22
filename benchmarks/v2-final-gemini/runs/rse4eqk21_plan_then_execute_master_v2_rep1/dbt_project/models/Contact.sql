{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    COALESCE(TRIM(mk.familienname), 'Unknown Contact Last Name') AS "LastName",
    mk.kontakt_email AS "Email",
    mk.tel AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mk.kd_nummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mku
ON
    mk.kd_nummer = mku.kundennummer
