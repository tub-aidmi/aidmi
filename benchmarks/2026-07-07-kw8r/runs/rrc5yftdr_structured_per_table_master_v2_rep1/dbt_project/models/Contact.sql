{{ config(materialized='table') }}

SELECT
    '003' || UPPER(SUBSTRING(md5(k.kontakt_id), 1, 17)) AS "Id",
    k.rufname AS "FirstName",
    k.familienname AS "LastName",
    k.kontakt_email AS "Email",
    k.tel AS "Phone",
    k.berufsbezeichnung AS "Title",
    CASE UPPER(TRIM(k.rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'ENDANWENDER' THEN 'End User'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'SPONSOR' THEN 'Executive Sponsor'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNIKER' THEN 'Technical Contact'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(k.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'GERMAN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRANZOESISCH' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || UPPER(SUBSTRING(md5(m.kundennummer), 1, 17)) AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m
    ON k.kd_nummer = m.kundennummer