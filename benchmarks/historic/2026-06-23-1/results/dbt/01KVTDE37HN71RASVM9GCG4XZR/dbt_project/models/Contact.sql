{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer
    FROM {{ source('fixture_master_src', 'master_kontakte') }}
)

SELECT
    kontakt_id AS Id,
    rufname AS FirstName,
    familienname AS LastName,
    kontakt_email AS Email,
    tel AS Phone,
    berufsbezeichnung AS Title,
    CASE
        WHEN rolle = 'Entscheider' THEN 'Decision Maker'
        WHEN rolle = 'Decision Maker' THEN 'Decision Maker'
        WHEN rolle = 'End User' THEN 'End User'
        WHEN rolle = 'Technical Contact' THEN 'Technical Contact'
        WHEN rolle = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE 'End User'
    END AS Role__c,
    CASE
        WHEN UPPER(korrespondenzsprache) IN ('DEUTSCH', 'DE') THEN 'DE'
        WHEN UPPER(korrespondenzsprache) IN ('EN', 'ENG') THEN 'EN'
        WHEN UPPER(korrespondenzsprache) IN ('FR', 'FRENCH') THEN 'FR'
        WHEN UPPER(korrespondenzsprache) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(korrespondenzsprache) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE 'EN'
    END AS Preferred_Language__c,
    kd_nummer AS AccountId,
    kontakt_id AS Legacy_Contact_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0 AS IsDeleted
FROM contact_source