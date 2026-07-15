{{ config(materialized='table') }}

WITH project_close_dates AS (
    SELECT 
        opportunity_ref,
        MAX(go_live) AS max_go_live
    FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
    WHERE go_live IS NOT NULL
      AND go_live ~ '^\d{4}-\d{2}-\d{2}$'
    GROUP BY opportunity_ref
)

SELECT 
    CAST(o.id AS TEXT) AS "Id",
    COALESCE(TRIM(o.name), '') AS "Name",
    CASE o.stage
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN o.stage IN ('Closed Won', 'Closed Lost') THEN pcd.max_go_live
        ELSE NULL
    END AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    'EUR'::TEXT AS "CurrencyIsoCode",
    -- Derive AccountId: KD-XXXX → ACC-XXXX
    'ACC-' || REGEXP_REPLACE(o.customer_number, '^KD-', '') AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN project_close_dates pcd 
    ON o.id = pcd.opportunity_ref