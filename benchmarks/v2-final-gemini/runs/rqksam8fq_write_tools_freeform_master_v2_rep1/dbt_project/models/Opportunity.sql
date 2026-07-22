-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    MD5(opportunities.opp_kennung) AS "Id",
    opportunities.titel AS "Name",
    CASE UPPER(TRIM(opportunities.vertriebsphase))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- NOT NULL, sensible default
    END AS "StageName",
    CASE
        WHEN opportunities.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(opportunities.zieldatum, 'YYYY-MM-DD')
        WHEN opportunities.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(opportunities.zieldatum, 'DD.MM.YYYY')
        WHEN opportunities.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(opportunities.zieldatum, 'MM/DD/YYYY')
        WHEN opportunities.zieldatum ~ '^\d{8}$' THEN TO_DATE(opportunities.zieldatum, 'YYYYMMDD')
        ELSE CURRENT_DATE -- NOT NULL, sensible default if unparseable
    END AS "CloseDate",
    CASE
        WHEN opportunities.auftragswert ~ '^\s*[-+]?[0-9]*[.,]?[0-9]+\s*$' THEN
            CASE
                WHEN opportunities.auftragswert LIKE '%.%' AND opportunities.auftragswert LIKE '%,%' THEN
                    REPLACE(REPLACE(opportunities.auftragswert, '.', ''), ',', '.')::DOUBLE PRECISION
                WHEN opportunities.auftragswert LIKE '%,%' THEN
                    REPLACE(opportunities.auftragswert, ',', '.')::DOUBLE PRECISION
                ELSE
                    opportunities.auftragswert::DOUBLE PRECISION
            END
        ELSE NULL
    END AS "Amount",
    opportunities.waehrungscode AS "CurrencyIsoCode",
    MD5(opportunities.kunden_ref) AS "AccountId",
    opportunities.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opportunities