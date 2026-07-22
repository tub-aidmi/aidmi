{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(stage)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(stage)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stage)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stage)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stage)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stage)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(stage)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL constraint as per plan
    END AS "StageName",
    CAST('2000-01-01' AS TEXT) AS "CloseDate", -- Default for NOT NULL constraint as per plan
    amount AS "Amount",
    CAST('USD' AS TEXT) AS "CurrencyIsoCode", -- Default as no direct source
    customer_number AS "AccountId", -- Foreign key to Account.Id
    id AS "Legacy_Opportunity_ID__c", -- Direct map from source primary key
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
