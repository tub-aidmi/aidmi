{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id)::TEXT AS "Id",
    INITCAP(TRIM(kontakte.rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(kontakte.familienname)), 'Unknown Contact') AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    kontakte.tel AS "Phone",
    INITCAP(TRIM(kontakte.berufsbezeichnung)) AS "Title",
    CASE LOWER(TRIM(kontakte.rolle))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(kontakte.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer)::TEXT AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON kontakte.kd_nummer = kunden.kundennummer