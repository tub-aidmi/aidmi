{{ config(materialized='table') }}
SELECT
    id AS "Id",
    CASE
        WHEN name ~* '(Stiftung & Co\. KGaA|Stiftung & Co\. KG|AG & Co\. KGaA|AG & Co\. KG|GmbH & Co\. KGaA|GmbH & Co\. KG|GbR|KGaA|AG|KG|GmbH)$'
        THEN INITCAP(REGEXP_REPLACE(name, '^(.*?)(Stiftung & Co\. KGaA|Stiftung & Co\. KG|AG & Co\. KGaA|AG & Co\. KG|GmbH & Co\. KGaA|GmbH & Co\. KG|GbR|KGaA|AG|KG|GmbH)$', '\1')) || REGEXP_REPLACE(name, '^(.*?)(Stiftung & Co\. KGaA|Stiftung & Co\. KG|AG & Co\. KGaA|AG & Co\. KG|GmbH & Co\. KGaA|GmbH & Co\. KG|GbR|KGaA|AG|KG|GmbH)$', '\2')
        ELSE INITCAP(name)
    END AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(tier) = 'gold' THEN 'Gold'
        WHEN LOWER(tier) = 'silver' THEN 'Silver'
        WHEN LOWER(tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}