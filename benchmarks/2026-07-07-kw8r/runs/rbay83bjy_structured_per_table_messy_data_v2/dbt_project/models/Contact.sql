{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(INITCAP(c.firstname)) AS "FirstName",
    TRIM(INITCAP(c.lastname)) AS "LastName",
    CASE WHEN c.email = 'N/A' OR c.email IS NULL THEN NULL ELSE TRIM(c.email) END AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(INITCAP(c.title)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(c.role__c)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(c.role__c)) IN ('END USER', 'ENDANWENDER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(c.role__c)) IN ('TECHNICAL CONTACT', 'TECHNIKER', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(c.role__c)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN TRIM(c.role__c) IN ('', 'N/A') THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('FR', 'FRENCH', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('IT', 'ITALIAN') THEN 'IT'
        WHEN TRIM(c.preferred_language__c) IN ('', 'N/A') THEN NULL
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c