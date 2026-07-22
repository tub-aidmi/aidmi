{{ config(materialized='table') }}

WITH opportunity_data AS (
    SELECT
        c.chance_id,
        c.bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        c.kd_nr,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    '006' || SUBSTRING(MD5(od.chance_id), 1, 15) AS "Id",
    od.bezeichnung AS "Name",
    CASE 
        WHEN UPPER(TRIM(od.phase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(od.phase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(od.phase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(od.phase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(od.phase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(od.phase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(od.phase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(od.phase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(od.phase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(od.phase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN od.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN od.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    od.volumen AS "Amount",
    od.waehrung AS "CurrencyIsoCode",
    ai.account_id AS "AccountId",
    od.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_data od
LEFT JOIN account_ids ai ON od.kd_nr = ai.kunden_nr
