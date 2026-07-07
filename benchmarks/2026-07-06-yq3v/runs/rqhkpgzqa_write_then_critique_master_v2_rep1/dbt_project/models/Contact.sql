{{ config(materialized='table') }}

SELECT
    MD5(TRIM(mk.kontakt_id)) AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    COALESCE(TRIM(mk.familienname), '') AS "LastName",
    TRIM(mk.kontakt_email) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE
        WHEN UPPER(TRIM(mk.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(mk.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(mk.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(mk.rolle)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(mk.kd_nummer)) AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
