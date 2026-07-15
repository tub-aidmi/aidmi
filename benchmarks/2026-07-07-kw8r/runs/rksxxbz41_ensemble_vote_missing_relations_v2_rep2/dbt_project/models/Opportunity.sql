{{ config(materialized='table') }}

WITH opportunity_data AS (
  SELECT 
    o."id" AS opportunity_id,
    o."name" AS opportunity_name,
    o."stage" AS stage,
    o."amount" AS amount,
    o."customer_number" AS customer_number,
    o."account_name" AS account_name
  FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
),

account_mapping AS (
  SELECT 
    a."id" AS account_id,
    a."name" AS account_name
  FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
)

SELECT 
  od.opportunity_id AS "Id",
  od.opportunity_name AS "Name",
  CASE 
    WHEN UPPER(TRIM(od.stage)) = 'PROSPECTING' THEN 'Prospecting'
    WHEN UPPER(TRIM(od.stage)) = 'QUALIFICATION' THEN 'Qualification'
    WHEN UPPER(TRIM(od.stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
    WHEN UPPER(TRIM(od.stage)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
    WHEN UPPER(TRIM(od.stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
    WHEN UPPER(TRIM(od.stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
    WHEN UPPER(TRIM(od.stage)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
    WHEN UPPER(TRIM(od.stage)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
    WHEN UPPER(TRIM(od.stage)) = 'CLOSED WON' THEN 'Closed Won'
    WHEN UPPER(TRIM(od.stage)) = 'CLOSED LOST' THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  NULL::text AS "CloseDate",
  od.amount AS "Amount",
  NULL::text AS "CurrencyIsoCode",
  am.account_id AS "AccountId",
  od.customer_number AS "Legacy_Opportunity_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM opportunity_data od
LEFT JOIN account_mapping am ON UPPER(TRIM(od.account_name)) = UPPER(TRIM(am.account_name))