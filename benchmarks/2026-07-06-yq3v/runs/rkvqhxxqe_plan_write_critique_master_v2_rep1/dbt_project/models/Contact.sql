{{ config(materialized='table') }}

SELECT
    MD5(mk.kontakt_id) AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    COALESCE(TRIM(mk.familienname), 'Unknown Contact') AS "LastName",
    TRIM(mk.kontakt_email) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE LOWER(TRIM(mk.rolle))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(mk.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(mku.kundennummer) AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mku
ON
    mk.kd_nummer = mku.kundennummer
