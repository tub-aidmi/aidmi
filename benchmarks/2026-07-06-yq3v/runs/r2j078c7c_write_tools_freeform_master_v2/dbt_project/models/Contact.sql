{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
)
SELECT
    MD5(kontakt_id) AS "Id",
    rufname AS "FirstName",
    COALESCE(familienname, 'Unknown') AS "LastName",
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
        WHEN UPPER(korrespondenzsprache) = 'DE' THEN 'DE'
        WHEN UPPER(korrespondenzsprache) = 'EN' THEN 'EN'
        WHEN UPPER(korrespondenzsprache) = 'FR' THEN 'FR'
        WHEN UPPER(korrespondenzsprache) = 'ES' THEN 'ES'
        WHEN UPPER(korrespondenzsprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kd_nummer) AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_data
