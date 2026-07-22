{{ config(materialized='table') }}

WITH cleaned_kontakte AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer,
        -- Defaulting CreatedDate and LastModifiedDate as source doesn't provide
        CAST(CURRENT_TIMESTAMP AS TEXT) AS created_date,
        CAST(CURRENT_TIMESTAMP AS TEXT) AS last_modified_date
    FROM
        {{ source('fixture_master_v2_src', 'master_kontakte') }}
)
SELECT
    MD5(kontakt_id) AS "Id",
    rufname AS "FirstName",
    COALESCE(familienname, 'Unknown') AS "LastName", -- LastName is NOT NULL
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(rolle) = 'end user' THEN 'End User'
        WHEN LOWER(rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(korrespondenzsprache) = 'de' THEN 'DE'
        WHEN LOWER(korrespondenzsprache) = 'en' THEN 'EN'
        WHEN LOWER(korrespondenzsprache) = 'fr' THEN 'FR'
        WHEN LOWER(korrespondenzsprache) = 'es' THEN 'ES'
        WHEN LOWER(korrespondenzsprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kd_nummer) AS "AccountId", -- AccountId is derived from kd_nummer
    kontakt_id AS "Legacy_Contact_ID__c",
    created_date AS "CreatedDate",
    last_modified_date AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_kontakte
