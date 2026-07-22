{{ config(materialized='table') }}

WITH opportunity_source AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),
project_dates AS (
    -- Use the latest go_live date per opportunity as CloseDate proxy
    SELECT 
        opportunity_ref,
        MAX(go_live) AS max_go_live
    FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
    GROUP BY opportunity_ref
)

SELECT
    CAST(opp.id AS TEXT) AS "Id",
    opp.name AS "Name",
    CASE LOWER(TRIM(opp.stage))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    proj.max_go_live AS "CloseDate",
    CAST(opp.amount AS DOUBLE PRECISION) AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    -- Transform customer_number KD-XXXX to Salesforce-style ACC-XXXX
    'ACC-' || REGEXP_REPLACE(opp.customer_number, '^KD-', '') AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_source opp
LEFT JOIN project_dates proj
    ON opp.id = proj.opportunity_ref