{{ config(materialized='table') }}

SELECT
    k.kontakt_id::text AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    CASE UPPER(TRIM(k.rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(k.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || LPAD(ak.kundennummer::text, 15, '0') AS "AccountId",
    k.kontakt_id::text AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k

LEFT JOIN (
    SELECT DISTINCT
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
) AS ak ON ak.kundennummer = k.kd_nummer::text