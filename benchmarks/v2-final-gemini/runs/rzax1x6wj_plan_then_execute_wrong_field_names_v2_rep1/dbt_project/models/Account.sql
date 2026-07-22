-- depends_on: {{ ref("stg_kunden") }}

{{ config(materialized='table') }}

SELECT
    MD5(kunden_nr) AS "Id",
    COALESCE(TRIM(firmenname), 'Unknown Account Name') AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN LOWER(kategorie) = 'gold' THEN 'Gold'
        WHEN LOWER(kategorie) = 'silber' THEN 'Silver'
        WHEN LOWER(kategorie) = 'bronze' THEN 'Bronze'
        WHEN LOWER(kategorie) = 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    TRIM(branche) AS "Industry",
    TRIM(webseite) AS "Website",
    TRIM(ort) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}