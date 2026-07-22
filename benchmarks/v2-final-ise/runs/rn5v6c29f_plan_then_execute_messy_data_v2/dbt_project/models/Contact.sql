{{ config(materialized='table') }}

WITH cte_contact AS (
    SELECT * FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
),
cte_account AS (
    SELECT * FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT 
    TRIM(c.id) AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    INITCAP(TRIM(c.lastname)) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    c.phone AS "Phone",
    INITCAP(TRIM(c.title)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(c.role__c)) IN ('decision maker', 'end user', 'technical contact', 'executive sponsor') 
        THEN INITCAP(TRIM(c.role__c)) 
        ELSE NULL 
    END AS "Role__c",
    UPPER(TRIM(c.preferred_language__c)) AS "Preferred_Language__c",
    TRIM(a.id) AS "AccountId",
    TRIM(c.id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM cte_contact c
LEFT JOIN cte_account a ON TRIM(UPPER(c.accountid)) = TRIM(UPPER(a.id))