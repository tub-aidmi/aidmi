{{ config(materialized='table') }}

WITH account_mapping AS (
    SELECT 
        id AS account_id,
        legacy_customer_id__c AS legacy_customer_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT 
    c.id AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(c.lastname)), 'Unknown') AS "LastName",
    TRIM(c.email) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(c.title) AS "Title",
    CASE 
        WHEN LOWER(TRIM(c.role__c)) IN ('decision maker', 'decision_maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(c.role__c)) IN ('end user', 'end_user') THEN 'End User'
        WHEN LOWER(TRIM(c.role__c)) IN ('technical contact', 'technical_contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(c.role__c)) IN ('executive sponsor', 'executive_sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(c.preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    am.account_id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN account_mapping am ON c.accountid = am.account_id