{{ config(materialized='table') }}

SELECT 
  o.id AS "Id",
  COALESCE(TRIM(o.name), 'Unknown') AS "Name",
  COALESCE(
    CASE LOWER(TRIM(o.stage))
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
    END, 
    'Prospecting'
  ) AS "StageName",
  CURRENT_DATE::TEXT AS "CloseDate",
  CASE WHEN o.amount IS NOT NULL THEN CAST(o.amount AS DOUBLE PRECISION) ELSE NULL END AS "Amount",
  NULL AS "CurrencyIsoCode",
  a.id AS "AccountId",
  o.id AS "Legacy_Opportunity_ID__c",
  CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
  ON UPPER(TRIM(o.customer_number)) = UPPER(TRIM(a.id))