{{ config(materialized='table') }}

SELECT
    kontakt_id AS Id,
    INITCAP(TRIM(rufname)) AS FirstName,
    COALESCE(INITCAP(TRIM(familienname)), '') AS LastName,
    TRIM(kontakt_email) AS Email,
    TRIM(tel) AS Phone,
    INITCAP(TRIM(berufsbezeichnung)) AS Title,

    CASE 
        WHEN UPPER(TRIM(COALESCE(rolle, ''))) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(rolle, ''))) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(COALESCE(rolle, ''))) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(COALESCE(rolle, ''))) = 'ENTSCHEIDER' THEN 'Executive Sponsor'
        ELSE NULL
    END AS Role__c,

    CASE 
        WHEN UPPER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(korrespondenzsprache, ''))) = 'FR' THEN 'FR'
        ELSE NULL
    END AS Preferred_Language__c,

    kd_nummer AS AccountId,
    kontakt_id AS Legacy_Contact_ID__c,
    CURRENT_DATE::TEXT AS CreatedDate,
    CURRENT_DATE::TEXT AS LastModifiedDate,
    0 AS IsDeleted

FROM {{ source('fixture_master_src', 'master_kontakte') }}