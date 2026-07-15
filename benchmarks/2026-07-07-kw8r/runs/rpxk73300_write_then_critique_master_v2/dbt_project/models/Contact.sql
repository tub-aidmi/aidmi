{{ config(materialized='table') }}

SELECT
    k.kontakt_id AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    INITCAP(COALESCE(TRIM(k.familienname), '')) AS "LastName",
    CASE
        WHEN k.kontakt_email IS NOT NULL AND k.kontakt_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            THEN LOWER(TRIM(k.kontakt_email))
        ELSE NULL
    END AS "Email",
    TRIM(k.tel) AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    CASE LOWER(TRIM(COALESCE(k.rolle, '')))
        WHEN 'decision maker'      THEN 'Decision Maker'
        WHEN 'end user'            THEN 'End User'
        WHEN 'endanwender'         THEN 'End User'
        WHEN 'technical contact'   THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'techniker'           THEN 'Technical Contact'
        WHEN 'executive sponsor'   THEN 'Executive Sponsor'
        WHEN 'sponsor'             THEN 'Executive Sponsor'
        WHEN 'entscheider'         THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(k.korrespondenzsprache, '')))
        WHEN 'DE'     THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'GERMAN'  THEN 'DE'
        WHEN 'EN'      THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FR'      THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'FRENCH'  THEN 'FR'
        WHEN 'ES'      THEN 'ES'
        WHEN 'IT'      THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN kdn.kundennummer IS NOT NULL
            THEN '001' || LPAD(REGEXP_REPLACE(kdn.kundennummer, '[^0-9]', '', 'g'), 15, '0')
        ELSE NULL
    END AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} kdn
    ON k.kd_nummer = kdn.kundennummer