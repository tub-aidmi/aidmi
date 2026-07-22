{{ config(materialized='table') }}

SELECT
    kontakt_id AS "Id",
    rufname AS "FirstName",
    familienname AS "LastName",
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE
        WHEN UPPER(TRIM(rolle)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(rolle)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(rolle)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(rolle)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kd_nummer AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }}
