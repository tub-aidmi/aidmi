{{ config(materialized='table') }}

SELECT 
    k.kontakt_id AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(k.familienname)), 'Unknown') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",

    CASE 
        WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNISCHER ANSPRECHPARTNER', 'TECHNIKER', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        ELSE NULL
    END AS "Role__c",

    CASE 
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('fr', 'french', 'français', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('es', 'spanish', 'español', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- Match Account.Id transform exactly
    '001' || LOWER(REGEXP_REPLACE(c.kundennummer, '[^a-z0-9]', '', 'g')) AS "AccountId",

    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
     0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
    ON TRIM(k.kd_nummer) = TRIM(c.kundennummer)