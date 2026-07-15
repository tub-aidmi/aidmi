{{ config(materialized='table') }}

SELECT 
    'CONT-' || mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    INITCAP(TRIM(mk.familienname)) AS "LastName",
    LOWER(mk.kontakt_email) AS "Email",
    TRIM(mk.tel) AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE 
        WHEN mk.rolle = 'Entscheider' THEN 'Decision Maker'
        WHEN mk.rolle LIKE '%Technik%' THEN 'Technical Contact'
        WHEN mk.rolle IN ('Entscheidungsträger', 'Executive Sponsor') THEN 'Executive Sponsor'
        ELSE 'End User'
    END AS "Role__c",
    CASE 
        WHEN LOWER(mk.korrespondenzsprache) LIKE '%deutsch%' THEN 'DE'
        WHEN LOWER(mk.korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(mk.korrespondenzsprache) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(mk.korrespondenzsprache) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(mk.korrespondenzsprache) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    'CUST-' || mku.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mku
    ON TRIM(mk.kd_nummer) = TRIM(mku.kundennummer)