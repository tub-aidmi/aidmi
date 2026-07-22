{{ config(materialized='table') }}

SELECT 
    c.chance_id AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE 
        WHEN LOWER(TRIM(c.phase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(c.phase)) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(c.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(c.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(c.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(c.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(c.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(c.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(c.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(c.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        ELSE 'Needs Analysis'
    END AS "StageName",
    COALESCE(
        CASE 
            WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' 
                THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN c.abschlussdatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' 
                THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN c.abschlussdatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
                THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN c.abschlussdatum ~ '^\d{8}$' 
                THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        END,
        '1900-01-01'
    ) AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(c.waehrung)) AS "CurrencyIsoCode",
    CASE 
        WHEN k.kunden_nr IS NOT NULL THEN '001' || REGEXP_REPLACE(k.kunden_nr, '\D', '', 'g')
        ELSE NULL
    END AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON c.kd_nr = k.kunden_nr