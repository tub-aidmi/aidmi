{{ config(materialized='table') }}

-- Opportunity model: parse dates, map stages, handle amounts/currencies
with source as (
    select *
    from {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),

parsed as (
    select
        -- Id: opportunity primary key
        "id" AS "Id",

        -- Name: required, fallback to 'Unknown' if empty
        CASE
            WHEN COALESCE(TRIM("name"), '') = '' THEN 'Unknown'
            ELSE TRIM("name")
        END AS "Name",

        -- StageName: map source stage values to allowed enum
        CASE UPPER(TRIM(COALESCE("stage", '')))
            WHEN 'PROSPECTING' THEN 'Prospecting'
            WHEN 'QUALIFICATION' THEN 'Qualification'
            WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
            WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
            WHEN 'CLOSED WON' THEN 'Closed Won'
            WHEN 'CLOSED LOST' THEN 'Closed Lost'
            WHEN 'NEW' THEN 'Prospecting'
            WHEN 'OPEN' THEN 'Prospecting'
            WHEN 'WON' THEN 'Closed Won'
            WHEN 'LOST' THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",

        -- CloseDate: source has no date column — default to NULL.
        -- If stage is closed, we could derive from stage name but source lacks dates.
        NULL AS "CloseDate",

        -- Amount: direct cast (already double precision in source)
        CASE WHEN CAST("amount" AS DOUBLE PRECISION) IS NOT NULL
            THEN "amount"
            ELSE NULL
        END AS "Amount",

        -- CurrencyIsoCode: not specified per row, default to USD
        'USD' AS "CurrencyIsoCode",

        -- AccountId: map opportunity.customer_number to account id format.
        -- Use direct mapping assuming customer_number corresponds to account ids.
        CASE
            WHEN COALESCE(TRIM("customer_number"), '') = '' THEN NULL
            ELSE TRIM("customer_number")
        END AS "AccountId",

        -- Legacy_Opportunity_ID__c: from source natural key
        "id" AS "Legacy_Opportunity_ID__c",

        -- CreatedDate: not in source, default NULL
        NULL AS "CreatedDate",

        -- LastModifiedDate: not in source, default NULL
        NULL AS "LastModifiedDate",

        -- IsDeleted: default 0 (active)
        0 AS "IsDeleted"

    from source
)

select * from parsed
