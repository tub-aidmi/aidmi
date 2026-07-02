{{ config(materialized='table') }}

SELECT
    m_kontakt.kontakt_id AS "Id",
    m_kontakt.rufname AS "FirstName",
    COALESCE(m_kontakt.familienname, 'Unknown') AS "LastName",
    m_kontakt.kontakt_email AS "Email",
    CASE
        WHEN TRIM(m_kontakt.tel) = '' OR UPPER(TRIM(m_kontakt.tel)) = 'N/A' THEN NULL
        ELSE TRIM(m_kontakt.tel)
    END AS "Phone",
    m_kontakt.berufsbezeichnung AS "Title",
    CASE
        WHEN UPPER(TRIM(COALESCE(m_kontakt.rolle, ''))) = 'DECISION MAKER' OR UPPER(TRIM(COALESCE(m_kontakt.rolle, ''))) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(m_kontakt.rolle, ''))) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(COALESCE(m_kontakt.rolle, ''))) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(COALESCE(m_kontakt.korrespondenzsprache, ''))) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(m_kontakt.korrespondenzsprache, ''))) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(m_kontakt.korrespondenzsprache, ''))) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(COALESCE(m_kontakt.korrespondenzsprache, ''))) IN ('ES', 'ESPANOL', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(COALESCE(m_kontakt.korrespondenzsprache, ''))) IN ('IT', 'ITALIANO', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    m_kunden.kundennummer AS "AccountId",
    m_kontakt.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_kontakte') }} m_kontakt
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} m_kunden
    ON TRIM(m_kontakt.kd_nummer) = TRIM(m_kunden.kundennummer)