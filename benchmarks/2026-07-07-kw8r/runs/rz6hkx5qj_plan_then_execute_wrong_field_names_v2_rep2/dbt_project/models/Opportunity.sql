{{ config(materialized='table') }}

SELECT
    '006' || TRIM(c.chance_id) AS "Id",
    COALESCE(INITCAP(TRIM(c.bezeichnung)), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM(c.phase)) IN ('prospecting', 'discovery', 'lead') THEN 'Prospecting'
        WHEN LOWER(TRIM(c.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) IN ('needs analysis', 'requirements', 'analyzing') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('value proposition', 'proving value', 'proposal') THEN 'Value Proposition'
        WHEN LOWER(TRIM(c.phase)) IN ('decision makers', 'identifying dm', 'key contacts') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c.phase)) IN ('perception analysis', 'mental map', 'comparison') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c.phase)) IN ('proposal price quote', 'pricing', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c.phase)) IN ('negotiation review', 'negotiating', 'final review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(c.phase)) IN ('closed won', 'won', 'accepted') THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) IN ('closed lost', 'lost', 'rejected') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum IS NOT NULL AND TRIM(c.abschlussdatum) != ''
             AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
            TO_DATE(TRIM(c.abschlussdatum), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    INITCAP(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    '001' || TRIM(k.kunden_nr) AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)