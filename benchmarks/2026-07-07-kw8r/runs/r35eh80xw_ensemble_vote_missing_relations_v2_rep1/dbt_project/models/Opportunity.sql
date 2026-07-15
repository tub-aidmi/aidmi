{{ config(materialized='table') }}

SELECT
    CAST(op."id" AS TEXT)                                                                                    AS "Id",
    COALESCE(NULLIF(TRIM(op."name"), ''), 'Unnamed Opportunity')                                             AS "Name",
    CASE LOWER(TRIM(op."stage"))
        WHEN 'prospecting'             THEN 'Prospecting'
        WHEN 'qualification'           THEN 'Qualification'
        WHEN 'needs analysis'          THEN 'Needs Analysis'
        WHEN 'value proposition'       THEN 'Value Proposition'
        WHEN 'id. decision makers'     THEN 'Id. Decision Makers'
        WHEN 'perception analysis'     THEN 'Perception Analysis'
        WHEN 'proposal/price quote'    THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review'      THEN 'Negotiation/Review'
        WHEN 'closed won'            THEN 'Closed Won'
        WHEN 'closed lost'           THEN 'Closed Lost'
        ELSE NULL
    END                                                                                                      AS "StageName",
    NULL                                                                                                     AS "CloseDate",
    CAST(op."amount" AS DOUBLE PRECISION)                                                                  AS "Amount",
    'USD'                                                                                                    AS "CurrencyIsoCode",
    acct."id"                                                                                                AS "AccountId",
    op."id"                                                                                                  AS "Legacy_Opportunity_ID__c",
    NULL                                                                                                     AS "CreatedDate",
    NULL                                                                                                     AS "LastModifiedDate",
    0                                                                                                        AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} op

LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct
    ON LOWER(TRIM(acct."id")) = LOWER(TRIM(op."customer_number"))