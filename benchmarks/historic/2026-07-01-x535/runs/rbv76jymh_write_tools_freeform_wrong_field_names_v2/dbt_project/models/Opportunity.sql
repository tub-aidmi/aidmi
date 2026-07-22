{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id) AS "Id",
    COALESCE(chancen.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(chancen.phase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(chancen.phase) = 'qualification' THEN 'Qualification'
        WHEN LOWER(chancen.phase) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(chancen.phase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(chancen.phase) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(chancen.phase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(chancen.phase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(chancen.phase) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(chancen.phase) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(chancen.phase) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        CASE
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(chancen.abschlussdatum::DATE, 'YYYY-MM-DD') -- YYYY-MM-DD
            WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            WHEN chancen.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            ELSE NULL
        END,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') -- Default for NOT NULL target if parsing fails
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    MD5(chancen.kd_nr) AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
