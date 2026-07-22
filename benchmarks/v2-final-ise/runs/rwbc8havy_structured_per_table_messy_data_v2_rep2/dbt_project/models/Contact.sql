{{ config(materialized='table') }}

SELECT
    '003' || SUBSTRING(id FROM 5) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), '') AS "LastName",
    CASE
        WHEN TRIM(email) IN ('N/A', '', NULL) THEN NULL
        ELSE TRIM(email)
    END AS "Email",
    CASE
        WHEN TRIM(phone) IN ('N/A', '') THEN NULL
        ELSE TRIM(phone)
    END AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN TRIM(accountid) IS NOT NULL AND accountid <> ''
        THEN '001' || SUBSTRING(accountid FROM 6)
        ELSE NULL
    END AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }}