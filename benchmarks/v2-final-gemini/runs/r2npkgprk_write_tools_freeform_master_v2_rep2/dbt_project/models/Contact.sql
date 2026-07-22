{{ config(materialized='table') }}

SELECT
    MD5(mc.kontakt_id) AS "Id",
    mc.rufname AS "FirstName",
    COALESCE(mc.familienname, 'Unknown') AS "LastName",
    mc.kontakt_email AS "Email",
    mc.tel AS "Phone",
    mc.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(mc.rolle) LIKE '%decision maker%' THEN 'Decision Maker'
        WHEN LOWER(mc.rolle) LIKE '%end user%' THEN 'End User'
        WHEN LOWER(mc.rolle) LIKE '%technical contact%' THEN 'Technical Contact'
        WHEN LOWER(mc.rolle) LIKE '%executive sponsor%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(mc.korrespondenzsprache) = 'DE' THEN 'DE'
        WHEN UPPER(mc.korrespondenzsprache) = 'EN' THEN 'EN'
        WHEN UPPER(mc.korrespondenzsprache) = 'FR' THEN 'FR'
        WHEN UPPER(mc.korrespondenzsprache) = 'ES' THEN 'ES'
        WHEN UPPER(mc.korrespondenzsprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(mk.kundennummer) AS "AccountId",
    mc.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} mc
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} mk
ON
    mc.kd_nummer = mk.kundennummer
