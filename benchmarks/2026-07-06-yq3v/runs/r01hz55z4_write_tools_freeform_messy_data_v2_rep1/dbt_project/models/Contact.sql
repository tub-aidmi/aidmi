{{ config(materialized='table') }}

SELECT
    id AS "Id",
    firstname AS "FirstName",
    COALESCE(lastname, 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN LOWER(role__c) = 'decision maker' OR LOWER(role__c) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(role__c) = 'end user' OR LOWER(role__c) = 'endanwender' THEN 'End User'
        WHEN LOWER(role__c) = 'technical contact' OR LOWER(role__c) = 'techniker' OR LOWER(role__c) = 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN LOWER(role__c) = 'executive sponsor' OR LOWER(role__c) = 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(preferred_language__c) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(preferred_language__c) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(preferred_language__c) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CAST('{{ default_date }}' AS TEXT) AS "CreatedDate",
    CAST('{{ default_date }}' AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}