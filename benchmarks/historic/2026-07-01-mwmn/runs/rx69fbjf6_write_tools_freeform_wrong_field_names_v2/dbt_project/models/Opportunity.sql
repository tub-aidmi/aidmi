{{ config(materialized='table') }}

SELECT
    "chance_id" AS "Id",
    "bezeichnung" AS "Name",
    CASE
        WHEN UPPER("phase") = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER("phase") = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER("phase") = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER("phase") = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER("phase") = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER("phase") = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER("phase") = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER("phase") = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER("phase") = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER("phase") = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    "abschlussdatum" AS "CloseDate",
    "volumen" AS "Amount",
    "waehrung" AS "CurrencyIsoCode",
    "kd_nr" AS "AccountId",
    "chance_id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
