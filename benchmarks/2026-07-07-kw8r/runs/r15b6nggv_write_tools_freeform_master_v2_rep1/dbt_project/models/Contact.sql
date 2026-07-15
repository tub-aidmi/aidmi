{{ config(materialized='table') }}

WITH contact_data AS (
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
),

account_mapping AS (
    SELECT
        kundennummer AS "AccountId",
        kundennummer AS "Legacy_Customer_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    c.kontakt_id AS "Id",
    INITCAP(TRIM(c.rufname)) AS "FirstName",
    INITCAP(TRIM(c.familienname)) AS "LastName",
    LOWER(TRIM(c.kontakt_email)) AS "Email",
    c.tel AS "Phone",
    INITCAP(TRIM(c.berufsbezeichnung)) AS "Title",
    CASE
        WHEN UPPER(TRIM(c.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(c.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(c.rolle)) IN ('TECHNICAL CONTACT', 'TECHNIKER', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(c.rolle)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    am."AccountId",
    c.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_data c
LEFT JOIN account_mapping am ON c.kd_nummer = am."Legacy_Customer_ID__c"
