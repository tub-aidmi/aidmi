-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(opp.stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(opp.stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(opp.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(opp.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(opp.stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(opp.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(opp.stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(opp.stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(opp.stage) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' if stage is unrecognized or NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live
            ELSE NULL -- Prefer NULL for unparseable dates if target allows, but CloseDate is NOT NULL
        END,
        '1900-01-01' -- Fallback to a fixed default for NOT NULL target column when source is unparseable
    ) AS "CloseDate",
    opp.amount AS "Amount",
    NULL::text AS "CurrencyIsoCode", -- No direct source for currency; explicitly cast NULL
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate", -- No direct source; explicitly cast NULL
    NULL::text AS "LastModifiedDate", -- No direct source; explicitly cast NULL
    0 AS "IsDeleted" -- Default to not deleted
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON opp.id = proj.opportunity_ref
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON opp.customer_number = acc.id