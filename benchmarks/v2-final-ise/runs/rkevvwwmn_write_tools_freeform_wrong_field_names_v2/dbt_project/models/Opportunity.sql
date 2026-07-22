{{ config(materialized='table') }}

SELECT
    '006' || MD5(c.chance_id) AS "Id",
    TRIM(c.bezeichnung) AS "Name",
    CASE 
        WHEN UPPER(c.phase) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(c.phase) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(c.phase) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(c.phase) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(c.phase) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(c.phase) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(c.phase) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(c.phase) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(c.phase) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(c.phase) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    UPPER(c.waehrung) AS "CurrencyIsoCode",
    '001' || MD5(k.kunden_nr) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr
