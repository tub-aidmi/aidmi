{{ config(materialized='table') }}
SELECT 
    o.id AS "Id",
    o.name AS "Name",
    COALESCE(
        CASE 
            WHEN o.stage IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN o.stage
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    REPLACE(o.customer_number, 'KD-', 'ACC-') AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o