{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    COALESCE(TRIM(mk.familienname), mk.kontakt_id) AS "LastName",
    TRIM(LOWER(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(mk.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(mk.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(mk.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(mk.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(mk.korrespondenzsprache) = 'DE' THEN 'DE'
        WHEN UPPER(mk.korrespondenzsprache) = 'EN' THEN 'EN'
        WHEN UPPER(mk.korrespondenzsprache) = 'FR' THEN 'FR'
        WHEN UPPER(mk.korrespondenzsprache) = 'ES' THEN 'ES'
        WHEN UPPER(mk.korrespondenzsprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mku.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mku
    ON mk.kd_nummer = mku.kundennummer
