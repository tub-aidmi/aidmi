-- dbt model for Opportunity

{{ config(materialized='table') }}

WITH 

opportunities_raw AS (
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

kunden_raw AS (
    SELECT
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    SUBSTRING(MD5(op.opp_kennung), 1, 15) || 'AAA' AS "Id",
    COALESCE(TRIM(INITCAP(op.titel)), op.opp_kennung) AS "Name",
    CASE
        WHEN TRIM(op.vertriebsphase) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(op.vertriebsphase) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' for NOT NULL constraint
    END AS "StageName",
    COALESCE(
        CASE
            WHEN op.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(op.zieldatum::DATE, 'YYYY-MM-DD')
            WHEN op.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(op.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN op.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(op.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN op.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(op.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        CURRENT_DATE::text -- Default to current date for NOT NULL constraint if unparseable
    ) AS "CloseDate",
    CASE
        WHEN op.auftragswert IS NULL OR op.auftragswert = 'None' THEN NULL
        ELSE
            CAST(REPLACE(REPLACE(UPPER(TRIM(REPLACE(op.auftragswert, 'EUR ', ''))), '.', ''), ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    UPPER(op.waehrungscode) AS "CurrencyIsoCode",
    SUBSTRING(MD5(kr.kundennummer), 1, 15) || 'AAA' AS "AccountId",
    op.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities_raw op
LEFT JOIN
    kunden_raw kr ON op.kunden_ref = kr.kundennummer