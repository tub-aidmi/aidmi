
{{ config(materialized='table') }}

SELECT
    k.kontakt_id AS "Id",
    TRIM(INITCAP(k.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(k.familienname)), 'N/A') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    k.tel AS "Phone",
    TRIM(INITCAP(k.berufsbezeichnung)) AS "Title",
    CASE
        WHEN UPPER(TRIM(k.rolle)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ku.kundennummer AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS k
LEFT JOIN
    {{ source('fixture_master_src', 'master_kunden') }} AS ku
ON
    k.kd_nummer = ku.kundennummer