{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(c.firstname) AS "FirstName",
    COALESCE(TRIM(c.lastname), 'Unknown') AS "LastName",
    NULLIF(TRIM(c.email), 'N/A') AS "Email",
    c.phone AS "Phone",
    c.title AS "Title",
    CASE 
        WHEN UPPER(TRIM(c.role__c)) IN ('DECISION MAKER', 'ENTSCHEIDER', 'ENTSCHEIDERIN') THEN 'Decision Maker'
        WHEN UPPER(TRIM(c.role__c)) IN ('END USER', 'ENDANWENDER', 'ENDANWENDERIN') THEN 'End User'
        WHEN UPPER(TRIM(c.role__c)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHE ANSPRECHPARTNERIN', 'TECHNICIAN', 'TECHNIKER', 'TECHNIKERIN') THEN 'Technical Contact'
        WHEN UPPER(TRIM(c.role__c)) IN ('EXECUTIVE SPONSOR', 'SPONSOR', 'EXECUTIVE') THEN 'Executive Sponsor'
        WHEN c.role__c IS NULL OR TRIM(c.role__c) = '' OR UPPER(TRIM(c.role__c)) = 'N/A' THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('FR', 'FRENCH', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON c.accountid = a.id
