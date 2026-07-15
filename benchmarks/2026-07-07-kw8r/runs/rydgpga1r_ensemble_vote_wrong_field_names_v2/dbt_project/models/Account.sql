{{ config(materialized='table') }}

SELECT
    -- Generate Salesforce-style 18-char Account Id: '001' prefix + deterministic digest
    CONCAT('001', LEFT(MD5(kunden_nr), 12)) AS "Id",
    -- Name from firmenname; default to '' if missing (NOT NULL constraint)
    COALESCE(TRIM(firmenname), '') AS "Name",
    -- ERP Number from source
    TRIM(erp_nummer) AS "ERP_Number__c",
    -- Customer Tier mapping from kategorie (assumed numeric or category labels -> Salesforce tier enum)
    CASE
        WHEN LOWER(TRIM(kategorie)) IN ('platinum', 'premium', '1') THEN 'Platinum'
        WHEN LOWER(TRIM(kategorie)) IN ('gold', 'standard', '2')   THEN 'Gold'
        WHEN LOWER(TRIM(kategorie)) IN ('silver', 'basic', '3')    THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) IN ('bronze', 'entry', '4')    THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    -- Region from gebiet
    TRIM(gebiet) AS "Region__c",
    -- Industry from branche (normalize with INITCAP)
    INITCAP(TRIM(branche)) AS "Industry",
    -- Website
    TRIM(webseite) AS "Website",
    -- BillingCity from ort
    TRIM(ort) AS "BillingCity",
    -- BillingCountry from land
    TRIM(land) AS "BillingCountry",
    -- Legacy source natural key preserved for verification
    kunden_nr AS "Legacy_Customer_ID__c",
    -- Synthetic dates (not present in source); use current date / constant
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    -- 0 = not deleted
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}