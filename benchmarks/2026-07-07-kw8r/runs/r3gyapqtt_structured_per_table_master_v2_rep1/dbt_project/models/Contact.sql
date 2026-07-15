{{ config(materialized='table') }}

SELECT
    '003' || SUBSTRING(MD5(k."kontakt_id") FROM 1 FOR 15) AS "Id",
    TRIM(INITCAP(k."rufname")) AS "FirstName",
    TRIM(INITCAP(k."familienname")) AS "LastName",
    LOWER(TRIM(k."kontakt_email")) AS "Email",
    TRIM(k."tel") AS "Phone",
    TRIM(INITCAP(k."berufsbezeichnung")) AS "Title",
    CASE
        WHEN UPPER(TRIM(k."rolle")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k."rolle")) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(k."rolle")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k."rolle")) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN TRIM(k."rolle") = '' OR k."rolle" IS NULL THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('IT', 'ITALIENISCH') THEN 'IT'
        WHEN TRIM(k."korrespondenzsprache") = '' OR k."korrespondenzsprache" IS NULL THEN NULL
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN k."kd_nummer" IS NOT NULL THEN '001' || SUBSTRING(MD5(k."kd_nummer") FROM 1 FOR 15)
        ELSE NULL
    END AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k