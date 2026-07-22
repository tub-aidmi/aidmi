{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(c.firstname) AS "FirstName",
    TRIM(c.lastname) AS "LastName",
    NULLIF(TRIM(c.email), 'N/A') AS "Email",
    c.phone AS "Phone",
    INITCAP(TRIM(c.title)) AS "Title",
    CASE
        WHEN UPPER(TRIM(c.role__c)) IN ('DECISION MAKER', 'ENTSCHEIDER', 'DECISIONMAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(c.role__c)) IN ('END USER', 'ENDANWENDER', 'ENDUSER') THEN 'End User'
        WHEN UPPER(TRIM(c.role__c)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHERANSPRECHPARTNER', 'TECHNISCHER KONTAKT', 'TECHNIKER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(c.role__c)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN c.role__c IS NULL OR c.role__c = '' OR c.role__c = 'N/A' THEN NULL
        ELSE 'End User'
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
        WHEN c.preferred_language__c IS NULL OR c.preferred_language__c = '' THEN NULL
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c