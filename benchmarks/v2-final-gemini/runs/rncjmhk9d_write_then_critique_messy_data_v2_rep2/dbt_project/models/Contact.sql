-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    c.firstname AS "FirstName",
    COALESCE(c.lastname, 'Unknown') AS "LastName",
    c.email AS "Email",
    c.phone AS "Phone",
    c.title AS "Title",
    CASE
        WHEN LOWER(c.role__c) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(c.role__c) = 'end user' THEN 'End User'
        WHEN LOWER(c.role__c) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(c.role__c) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS c
