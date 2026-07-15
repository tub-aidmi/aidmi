{{ config(materialized='table') }}

WITH account_keys AS (
    SELECT DISTINCT
        '001' || RIGHT('000000' || REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'), 6) AS sf_account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    '001' || RIGHT('000000' || REGEXP_REPLACE(a.kunden_nr, '[^0-9]', '', 'g'), 6) AS "Id",
    a.firmenname AS "Name",
    CAST(NULLIF(a.erp_nummer, '') AS TEXT) AS "ERP_Number__c",
    a.kategorie AS "Customer_Tier__c",
    a.gebiet AS "Region__c",
    a.branche AS "Industry",
    CAST(NULLIF(a.webseite, '') AS TEXT) AS "Website",
    a.ort AS "BillingCity",
    a.land AS "BillingCountry",
    a.kunden_nr AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} a
