{{ config(materialized='table') }}

WITH src_contact AS (
    SELECT * FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
),
src_account AS (
    SELECT id FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT
    UPPER(REGEXP_REPLACE(TRIM(sc.id), '^[^a-zA-Z0-9]+', '', 'g')) AS "Id",
    INITCAP(TRIM(sc.firstname)) AS "FirstName",
    INITCAP(COALESCE(TRIM(sc.lastname), 'Unknown')) AS "LastName",
    LOWER(TRIM(sc.email)) AS "Email",
    TRIM(sc.phone) AS "Phone",
    INITCAP(TRIM(sc.title)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(sc.role__c)) IN ('decision maker', 'end user', 'technical contact', 'executive sponsor') 
        THEN INITCAP(TRIM(sc.role__c)) 
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(sc.preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') 
        THEN UPPER(TRIM(sc.preferred_language__c)) 
        ELSE NULL 
    END AS "Preferred_Language__c",
    sa.id AS "AccountId",
    sc.id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM src_contact sc
LEFT JOIN src_account sa 
    ON UPPER(REGEXP_REPLACE(TRIM(sc.accountid), '^[^a-zA-Z0-9]+', '', 'g')) = 
       UPPER(REGEXP_REPLACE(TRIM(sa.id), '^[^a-zA-Z0-9]+', '', 'g'))