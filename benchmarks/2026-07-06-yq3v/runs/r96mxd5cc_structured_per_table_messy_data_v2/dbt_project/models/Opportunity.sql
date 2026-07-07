-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    opportunity.id AS "Id",
    COALESCE(TRIM(opportunity.name), 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('id. decision makers', 'id decision makers', 'id_decision_makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('proposal/price quote', 'proposal price quote', 'proposal_price_quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('negotiation/review', 'negotiation review', 'negotiation_review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN TRIM(LOWER(opportunity.stagename)) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opportunity.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN opportunity.closedate
            WHEN opportunity.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN opportunity.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN TRIM(REPLACE(REPLACE(opportunity.amount, '.', ''), ',', '.')) ~ '^[-+]?[0-9]+(\.[0-9]+)?$'
            THEN CAST(TRIM(REPLACE(REPLACE(opportunity.amount, '.', ''), ',', '.')) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    opportunity.currencyisocode AS "CurrencyIsoCode",
    opportunity.accountid AS "AccountId",
    opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity