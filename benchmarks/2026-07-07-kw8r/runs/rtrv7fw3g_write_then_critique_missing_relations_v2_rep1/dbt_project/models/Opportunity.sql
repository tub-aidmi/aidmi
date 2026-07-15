{{ config(materialized='table') }}
WITH opportunity_project_dates AS (
  SELECT p.opportunity_ref AS opportunity_id,
         MIN(TO_DATE(p.go_live, 'YYYY-MM-DD')) AS earliest_go_live
  FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
  WHERE p.opportunity_ref IS NOT NULL
    AND p.go_live ~ '^\d{4}-\d{2}-\d{2}$'
  GROUP BY p.opportunity_ref
)
SELECT o.id AS "Id",
       o.name AS "Name",
       CASE WHEN UPPER(TRIM(o.stage)) = 'PROSPECTING' THEN 'Prospecting'
            WHEN UPPER(TRIM(o.stage)) = 'QUALIFICATION' THEN 'Qualification'
            WHEN UPPER(TRIM(o.stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
            WHEN UPPER(TRIM(o.stage)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN UPPER(TRIM(o.stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN UPPER(TRIM(o.stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            WHEN UPPER(TRIM(o.stage)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN UPPER(TRIM(o.stage)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
            WHEN UPPER(TRIM(o.stage)) = 'CLOSED WON' THEN 'Closed Won'
            WHEN UPPER(TRIM(o.stage)) = 'CLOSED LOST' THEN 'Closed Lost'
            ELSE NULL
       END AS "StageName",
       COALESCE(
         TO_CHAR(opd.earliest_go_live, 'YYYY-MM-DD'),
         TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
       ) AS "CloseDate",
       o.amount AS "Amount",
       NULL::text AS "CurrencyIsoCode",
       a.id AS "AccountId",
       o.id AS "Legacy_Opportunity_ID__c",
       NULL::text AS "CreatedDate",
       NULL::text AS "LastModifiedDate",
       0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
  ON REGEXP_REPLACE(o.customer_number, '^KD-(\d+)$', 'ACC-\1') = a.id
LEFT JOIN opportunity_project_dates opd ON o.id = opd.opportunity_id