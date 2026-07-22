{{ config(materialized='table') }}

SELECT
    kontakt_id AS "Id",
    TRIM(rufname) AS "FirstName",
    TRIM(familienname) AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE
        WHEN UPPER(TRIM(rolle)) IN ('ENTSCHEIDER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(rolle)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(rolle)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'FR' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(kd_nummer) AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }}
