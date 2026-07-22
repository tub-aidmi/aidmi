{{ config(materialized='table') }}

WITH opportunities_stg AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

customers_stg AS (
    SELECT
        kundennummer,
        unternehmensname
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(opp.titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN opp.vertriebsphase ILIKE '%Prospect%' THEN 'Prospecting'
        WHEN opp.vertriebsphase ILIKE '%Qualif%' THEN 'Qualification'
        WHEN opp.vertriebsphase ILIKE '%Needs%' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase ILIKE '%Value%' THEN 'Value Proposition'
        WHEN opp.vertriebsphase ILIKE '%Decision Maker%' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase ILIKE '%Perception%' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase ILIKE '%Proposal%' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase ILIKE '%Negotiation%' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase ILIKE '%Won%' THEN 'Closed Won'
        WHEN opp.vertriebsphase ILIKE '%Lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN opp.auftragswert ~ '^\d+(,\d{3})*\.\d{2}$' THEN CAST(REPLACE(opp.auftragswert, ',', '') AS DOUBLE PRECISION) -- US format
        WHEN opp.auftragswert ~ '^\d{1,3}(\.\d{3})*,\d{2}$' THEN CAST(REPLACE(REPLACE(opp.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION) -- European format
        WHEN opp.auftragswert ~ '^-?\d+(\.\d+)?$' THEN CAST(opp.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    MD5(cust.kundennummer) AS "AccountId", -- Placeholder for Salesforce-style Account ID
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    opportunities_stg opp
LEFT JOIN
    customers_stg cust ON opp.kunden_ref = cust.kundennummer
