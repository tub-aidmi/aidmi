{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    -- Salesforce-style Id: prefix customer number to resemble 15-char Salesforce IDs
    ('001' || REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g')) AS "Id",
    -- Account Name from company name
    firmenname AS "Name",
    -- ERP Number from source
    erp_nummer AS "ERP_Number__c",
    -- Customer Tier: map category values to Gold/Silver/Bronze/Platinum
    CASE LOWER(TRIM(kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE 'Bronze'
    END AS "Customer_Tier__c",
    -- Region from source gebiet field
    INITCAP(TRIM(gebiet)) AS "Region__c",
    -- Industry from source branche field
    INITCAP(TRIM(branche)) AS "Industry",
    -- Website from source webseite field
    webseite AS "Website",
    -- Billing City from source ort field
    INITCAP(TRIM(ort)) AS "BillingCity",
    -- Billing Country from source land field
    UPPER(TRIM(land)) AS "BillingCountry",
    -- Legacy Customer ID: original natural key for row-level verification
    kunden_nr AS "Legacy_Customer_ID__c",
    -- No source dates available; set to NULL
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    -- Not deleted by default
    0 AS "IsDeleted"

FROM source