{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, 'Unnamed Opportunity') AS "Name",
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
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    -- Date parsing: assuming YYYY-MM-DD, DD.MM.YYYY, YYYYMMDD, MM/DD/YYYY formats
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default for NOT NULL, in a real scenario, this would be a business decision.
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId", -- Maps to Account.Id (kunden_nr)
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
