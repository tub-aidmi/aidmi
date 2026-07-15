{{ config(materialized='table') }}

WITH opportunity_data AS (
    SELECT
        c.chance_id,
        c.bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        c.kd_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || LPAD(
            REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'),
            15,
            '0'
        ) AS account_sf_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    '006' || LPAD(
        REGEXP_REPLACE(o.chance_id, '[^0-9]', '', 'g'),
        15,
        '0'
    ) AS "Id",
    o.bezeichnung AS "Name",
    
    CASE 
        WHEN UPPER(o.phase) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(o.phase) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(o.phase) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(o.phase) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(o.phase) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(o.phase) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(o.phase) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(o.phase) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(o.phase) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(o.phase) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    
    CASE 
        WHEN o.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN o.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    
    o.volumen AS "Amount",
    o.waehrung AS "CurrencyIsoCode",
    a.account_sf_id AS "AccountId",
    o.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM opportunity_data o
LEFT JOIN account_ids a ON o.kd_nr = a.kunden_nr
