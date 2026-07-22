{{ config(materialized='table') }}

WITH opportunity_with_close_date AS (
    SELECT
        o.id AS opportunity_id,
        MIN(p.go_live) AS close_date -- Take the earliest go_live date if multiple projects for one opportunity
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
        ON o.id = p.opportunity_ref
    GROUP BY o.id
)
SELECT
    o.id AS "Id",
    COALESCE(o.name, o.id) AS "Name", -- Name is NOT NULL, use id as fallback
    CASE
        WHEN o.stage = 'Prospecting' THEN 'Prospecting'
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN o.stage = 'Value Proposition' THEN 'Value Proposition'
        WHEN o.stage = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN o.stage = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN o.stage = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN o.stage = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        (o_cd.close_date::DATE)::TEXT, -- Cast to DATE then TEXT (ISO YYYY-MM-DD)
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate", -- NOT NULL, fallback to current date
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default to USD
    acc.id AS "AccountId", -- Join to account to get Salesforce Id
    o.id AS "Legacy_Opportunity_ID__c", -- Source natural key
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate", -- Default to current datetime
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate", -- Default to current datetime
    0 AS "IsDeleted" -- Default to 0
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN opportunity_with_close_date AS o_cd
    ON o.id = o_cd.opportunity_id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON o.account_name = acc.name
