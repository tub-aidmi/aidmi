{{ config(materialized='table') }}

WITH account_map AS (
    SELECT
        '001' || TRIM(kundennummer) AS account_id,
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
contacts_raw AS (
    SELECT
        mk.kontakt_id,
        mk.rufname,
        mk.familienname,
        mk.kontakt_email,
        mk.tel,
        mk.berufsbezeichnung,
        mk.rolle,
        mk.korrespondenzsprache,
        mk.kd_nummer,
        am.account_id AS account_id_ref
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
    LEFT JOIN account_map am ON TRIM(mk.kd_nummer) = am.kundennummer
)

SELECT
    -- Id: Salesforce Contact format (003 prefix)
    '003' || TRIM(kontakt_id) AS "Id",
    
    -- FirstName from rufname
    INITCAP(TRIM(rufname)) AS "FirstName",
    
    -- LastName from familienname (NOT NULL — fallback to empty string per Salesforce convention)
    COALESCE(NULLIF(TRIM(familienname), ''), '') AS "LastName",
    
    -- Email
    LOWER(TRIM(kontakt_email)) AS "Email",
    
    -- Phone
    TRIM(tel) AS "Phone",
    
    -- Title from berufsbezeichnung
    INITCAP(TRIM(berufsbezeichnung)) AS "Title",
    
    -- Role__c: map German role values to enum (Decision Maker, End User, Technical Contact, Executive Sponsor)
    CASE
        WHEN UPPER(TRIM(rolle)) LIKE '%ENTSCHEIDER%' THEN 'Decision Maker'
        WHEN UPPER(TRIM(rolle)) LIKE '%ENDBENUTZER%' THEN 'End User'
        WHEN UPPER(TRIM(rolle)) LIKE '%TECHNISCH%' THEN 'Technical Contact'
        WHEN UPPER(TRIM(rolle)) LIKE '%EXECUTIVE SPONSOR%' OR UPPER(TRIM(rolle)) LIKE '%VORSITZ%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    
    -- Preferred_Language__c: normalize to ISO 2-letter code
    CASE
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'GERMAN', 'DEU') THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENG') THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRE', 'FRA') THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPA') THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITA') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    
    -- AccountId: Salesforce Account format (001 prefix) joined from master_kunden
    account_id_ref AS "AccountId",
    
    -- Legacy_Contact_ID__c: source natural key
    kontakt_id AS "Legacy_Contact_ID__c",
    
    -- CreatedDate / LastModifiedDate: not available in source
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    
    -- IsDeleted: default 0 (not deleted)
    0 AS "IsDeleted"

FROM contacts_raw;