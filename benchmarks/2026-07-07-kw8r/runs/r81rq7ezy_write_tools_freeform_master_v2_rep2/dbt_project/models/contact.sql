{{ config(materialized='table') }}

SELECT
    'C' || SUBSTR(MD5(src."kontakt_id"), 1, 14) AS "Id",
    src."rufname" AS "FirstName",
    src."familienname" AS "LastName",
    src."kontakt_email" AS "Email",
    src."tel" AS "Phone",
    src."berufsbezeichnung" AS "Title",
    CASE
        WHEN UPPER(TRIM(src."rolle")) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(src."rolle")) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(src."rolle")) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(src."rolle")) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(src."korrespondenzsprache")) IN ('DE', 'GERMAN', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(src."korrespondenzsprache")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(src."korrespondenzsprache")) IN ('FR', 'FRENCH', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM(src."korrespondenzsprache")) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(src."korrespondenzsprache")) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'A' || SUBSTR(MD5(src."kd_nummer"), 1, 14) AS "AccountId",
    src."kontakt_id" AS "Legacy_Contact_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} src
