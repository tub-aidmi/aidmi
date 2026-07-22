{{ config(materialized='table') }}

SELECT
    op."id"                                                                                       AS "Id",
    INITCAP(TRIM(op."name"))                                                                        AS "Name",
    CASE LOWER(TRIM(op."stage"))
        WHEN 'prospecting'             THEN 'Prospecting'
        WHEN 'qualification'           THEN 'Qualification'
        WHEN 'needs analysis'          THEN 'Needs Analysis'
        WHEN 'value proposition'       THEN 'Value Proposition'
        WHEN 'id. decision makers'    THEN 'Id. Decision Makers'
        WHEN 'perception analysis'     THEN 'Perception Analysis'
        WHEN 'proposal/price quote'   THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review'     THEN 'Negotiation/Review'
        WHEN 'closed won'             THEN 'Closed Won'
        WHEN 'closed lost'            THEN 'Closed Lost'
        ELSE NULL
    END                                                                                             AS "StageName",
    CASE
        -- Try common date formats for CloseDate — prefer earliest parseable
        WHEN TO_DATE(TRIM(op."close_date"), 'YYYY-MM-DD') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(op."close_date"), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN op."close_date" ~ '^\d{2}\.\d{2}\.\d{4}$'               THEN REPLACE(REPLACE(op."close_date", '.', '-'), '-', '-')
        ELSE NULL
    END                                                                                             AS "CloseDate",
    op."amount"                                                                                     AS "Amount",
    'USD'                                                                                           AS "CurrencyIsoCode",
    acct."id"                                                                                       AS "AccountId",
    op."id"                                                                                         AS "Legacy_Opportunity_ID__c",
    NULL                                                                                            AS "CreatedDate",
    NULL                                                                                            AS "LastModifiedDate",
    0                                                                                               AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} op

LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct
    ON LOWER(TRIM(acct."id")) = LOWER(TRIM(op."customer_number"))

WHERE TRIM(op."name") != '' OR op."name" IS NULL  -- keep all rows with or without a name; filter empty names if desired