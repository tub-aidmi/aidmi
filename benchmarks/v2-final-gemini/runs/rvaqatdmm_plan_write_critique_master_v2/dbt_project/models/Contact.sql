{{ config(materialized='table') }}

SELECT
    MD5(mkont.kontakt_id) AS "Id",
    INITCAP(TRIM(mkont.rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(mkont.familienname)), 'Unknown Contact ' || mkont.kontakt_id) AS "LastName",
    LOWER(TRIM(mkont.kontakt_email)) AS "Email",
    TRIM(mkont.tel) AS "Phone",
    TRIM(mkont.berufsbezeichnung) AS "Title",
    CASE LOWER(TRIM(mkont.rolle))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(mkont.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE 'EN'
    END AS "Preferred_Language__c",
    MD5(mkund.kundennummer) AS "AccountId",
    mkont.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mkont
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mkund
    ON mkont.kd_nummer = mkund.kundennummer
