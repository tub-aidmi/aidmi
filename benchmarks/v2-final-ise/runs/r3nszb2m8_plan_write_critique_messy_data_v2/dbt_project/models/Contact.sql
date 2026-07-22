{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(id)) AS "Id",
    COALESCE(INITCAP(TRIM(firstname)), '') AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'entscheider')   THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'endanwender')          THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor')     THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(preferred_language__c)) IN ('de', 'deutsch', 'german')                          THEN 'DE'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('en', 'englisch', 'english')                        THEN 'EN'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('fr', 'français', 'french')                         THEN 'FR'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('it', 'italienisch', 'italian')                     THEN 'IT'
        WHEN LOWER(TRIM(preferred_language__c)) IN ('es', 'español', 'spanish')                         THEN 'ES'
        ELSE NULL
    END AS "Preferred_Language__c",
    UPPER(TRIM(accountid)) AS "AccountId",
    TRIM(id) AS "Legacy_Contact_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}