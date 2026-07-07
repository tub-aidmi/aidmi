-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    TRIM(firstname) AS "FirstName",
    COALESCE(TRIM(lastname), 'Unknown') AS "LastName",
    TRIM(LOWER(email)) AS "Email",
    phone AS "Phone",
    TRIM(title) AS "Title",
    CASE
        WHEN LOWER(role__c) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(role__c) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(role__c) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(role__c) IN ('sponsor', 'executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(preferred_language__c) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(preferred_language__c) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(preferred_language__c) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(preferred_language__c) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(preferred_language__c) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
