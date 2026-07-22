 artichokes

{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, 'Unknown') AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN TRIM(LOWER(kontakte.rolle)) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(kontakte.rolle)) = 'end user' THEN 'End User'
        WHEN TRIM(LOWER(kontakte.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN TRIM(LOWER(kontakte.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(kontakte.korrespondenzsprache)) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN TRIM(UPPER(kontakte.korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN TRIM(UPPER(kontakte.korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(kontakte.korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(kontakte.korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakte.kd_nummer AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    CAST(NULL AS text) AS "CreatedDate",
    CAST(NULL AS text) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS kontakte