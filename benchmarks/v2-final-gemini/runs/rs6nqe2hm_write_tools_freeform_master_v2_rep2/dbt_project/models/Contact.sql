-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    COALESCE(mk.familienname, 'Unknown') AS "LastName", -- LastName is NOT NULL
    mk.kontakt_email AS "Email",
    mk.tel AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(mk.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(mk.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(mk.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(mk.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
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
    mak.kundennummer AS "AccountId", -- Join on kd_nummer and kundennummer
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mak
    ON mk.kd_nummer = mak.kundennummer
