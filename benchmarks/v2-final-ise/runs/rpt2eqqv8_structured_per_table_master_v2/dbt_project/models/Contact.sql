{{ config(materialized='table') }}

SELECT
    '003_' || MASTER_KONTAKTE.kontakt_id AS "Id",
    INITCAP(TRIM(MASTER_KONTAKTE.rufname)) AS "FirstName",
    INITCAP(TRIM(COALESCE(MASTER_KONTAKTE.familienname, 'Unknown'))) AS "LastName",
    LOWER(TRIM(MASTER_KONTAKTE.kontakt_email)) AS "Email",
    TRIM(MASTER_KONTAKTE.tel) AS "Phone",
    INITCAP(TRIM(MASTER_KONTAKTE.berufsbezeichnung)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(MASTER_KONTAKTE.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(MASTER_KONTAKTE.rolle)) IN ('end user', 'endanwender', 'nutzer') THEN 'End User'
        WHEN LOWER(TRIM(MASTER_KONTAKTE.rolle)) IN ('technical contact', 'technikkontakt', 'technisch kontakt') THEN 'Technical Contact'
        WHEN LOWER(TRIM(MASTER_KONTAKTE.rolle)) IN ('executive sponsor', 'vorstandssponsor', 'geschäftsführer') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(COALESCE(MASTER_KONTAKTE.korrespondenzsprache, ''))) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(MASTER_KONTAKTE.korrespondenzsprache, ''))) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(MASTER_KONTAKTE.korrespondenzsprache, ''))) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(COALESCE(MASTER_KONTAKTE.korrespondenzsprache, ''))) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(COALESCE(MASTER_KONTAKTE.korrespondenzsprache, ''))) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001_' || MASTER_KUNDEN.kundennummer AS "AccountId",
    MASTER_KONTAKTE.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} MASTER_KONTAKTE
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} MASTER_KUNDEN 
    ON MASTER_KONTAKTE.kd_nummer = MASTER_KUNDEN.kundennummer