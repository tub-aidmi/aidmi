{{ config(materialized='table') }}

SELECT
  CAST(o."id" AS TEXT) AS "Id",
  o."name" AS "Name",
  CASE
    WHEN UPPER(TRIM(o."stage")) = 'PROSPECTING' THEN 'Prospecting'
    WHEN UPPER(TRIM(o."stage")) = 'QUALIFICATION' THEN 'Qualification'
    WHEN UPPER(TRIM(o."stage")) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
    WHEN UPPER(TRIM(o."stage")) = 'VALUE PROPOSITION' THEN 'Value Proposition'
    WHEN UPPER(TRIM(o."stage")) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
    WHEN UPPER(TRIM(o."stage")) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
    WHEN UPPER(TRIM(o."stage")) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
    WHEN UPPER(TRIM(o."stage")) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
    WHEN UPPER(TRIM(o."stage")) = 'CLOSED WON' THEN 'Closed Won'
    WHEN UPPER(TRIM(o."stage")) = 'CLOSED LOST' THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  NULL AS "CloseDate",
  CAST(o."amount" AS DOUBLE PRECISION) AS "Amount",
  'USD' AS "CurrencyIsoCode",
  CASE
    WHEN o."customer_number" IS NOT NULL AND o."customer_number" ~ '^KD-\d+$' THEN REGEXP_REPLACE(o."customer_number", '^KD-', 'ACC-')
    ELSE NULL
  END AS "AccountId",
  CAST(o."id" AS TEXT) AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
