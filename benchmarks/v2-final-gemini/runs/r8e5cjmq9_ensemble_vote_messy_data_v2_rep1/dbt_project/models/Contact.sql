-- depends_on: {{ ref('account') }}
{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    src.firstname AS "FirstName",
    COALESCE(src.lastname, 'Unknown') AS "LastName",
    src.email AS "Email",
    src.phone AS "Phone",
    src.title AS "Title",
    CASE
        WHEN LOWER(TRIM(src.role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(src.role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(src.role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(src.role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(src.preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(src.preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} src
