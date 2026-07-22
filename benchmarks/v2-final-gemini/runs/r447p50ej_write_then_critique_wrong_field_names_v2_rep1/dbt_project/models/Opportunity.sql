{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(c.phase) IN ('prospecting', 'new') THEN 'Prospecting'
        WHEN LOWER(c.phase) IN ('qualification', 'qualifying') THEN 'Qualification'
        WHEN LOWER(c.phase) IN ('needs analysis', 'analysis') THEN 'Needs Analysis'
        WHEN LOWER(c.phase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(c.phase) IN ('id. decision makers', 'identification') THEN 'Id. Decision Makers'
        WHEN LOWER(c.phase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(c.phase) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(c.phase) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(c.phase) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(c.phase) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting for unmapped phases
    END AS "StageName",
    COALESCE(
        CASE
            WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Use current date as fallback for NOT NULL CloseDate
    ) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId", -- Corrected to match Account.Id directly
    c.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON c.kd_nr = k.kunden_nr