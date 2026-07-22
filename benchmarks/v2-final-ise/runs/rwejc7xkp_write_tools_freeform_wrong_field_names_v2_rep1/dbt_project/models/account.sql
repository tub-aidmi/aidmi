{{ config(materialized='table') }}

SELECT
      -- Generate Salesforce-style Account Id from customer number
    CONCAT('001', CASE WHEN kunden_nr ~ '^\d{5,}$' THEN SUBSTRING(kunden_nr FROM 2) ELSE LPAD(SUBSTRING(kunden_nr FROM '\d+'), 9, '0') END) AS "Id",

      -- Name: firmenname NOT NULL
    COALESCE(TRIM(firmenname), 'Unknown Company') AS "Name",

      -- ERP Number
    TRIM(erp_nummer) AS "ERP_Number__c",

      -- Customer Tier mapping from kategorie enum
    CASE LOWER(TRIM(kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",

      -- Region from gebiet
    INITCAP(TRIM(gebiet)) AS "Region__c",

      -- Industry from branche
    INITCAP(TRIM(branche)) AS "Industry",

      -- Website
    TRIM(webseite) AS "Website",

      -- BillingCity from ort
    INITCAP(TRIM(ort)) AS "BillingCity",

      -- BillingCountry from land
    INITCAP(TRIM(land)) AS "BillingCountry",

      -- Legacy Customer ID: original natural key
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",

      -- CreatedDate default
     '2024-01-01' AS "CreatedDate",

      -- LastModifiedDate default
     '2024-01-01' AS "LastModifiedDate",

      -- IsDeleted: 0 (not deleted)
     0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
