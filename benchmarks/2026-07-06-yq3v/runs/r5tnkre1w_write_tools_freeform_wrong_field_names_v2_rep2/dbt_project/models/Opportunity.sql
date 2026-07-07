{{ config(materialized='table') }}

SELECT
    MD5(chance_id) AS "Id",
    COALESCE(bezeichnung, 'Untitled Opportunity') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(phase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(phase) = 'qualification' THEN 'Qualification'
        WHEN LOWER(phase) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(phase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(phase) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(phase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(phase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(phase) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(phase) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(phase) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(abschlussdatum, 'DD.MM.YYYY')
            WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(abschlussdatum, 'YYYY-MM-DD') -- Assuming this could also be a format
            WHEN abschlussdatum ~ '^\d{8}$' THEN TO_DATE(abschlussdatum, 'YYYYMMDD')
            WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(abschlussdatum, 'MM/DD/YYYY')
            ELSE NULL
        END,
        '2000-01-01' -- Default date if unparseable, since CloseDate is NOT NULL
    )::TEXT AS "CloseDate", -- Cast to text for ISO YYYY-MM-DD output
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    MD5(kd_nr) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
