{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id) AS "Id",
    COALESCE(chancen.bezeichnung, 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
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
        ELSE 'Prospecting' -- StageName is NOT NULL, so provide a default
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD')
            WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY')
            WHEN chancen.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY')
            ELSE NULL
        END, 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- CloseDate is NOT NULL, provide default for unparseable
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    MD5(chancen.kd_nr) AS "AccountId", -- Links to kunden.kunden_nr
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
