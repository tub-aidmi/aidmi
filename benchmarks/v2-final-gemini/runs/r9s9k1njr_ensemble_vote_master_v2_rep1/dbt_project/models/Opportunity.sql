-- This dbt model transforms data from the master_opportunities source table into the Opportunity target schema.
-- It joins with master_kunden to resolve the AccountId.

{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    mo.titel AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) LIKE '%in kontakt%' THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) LIKE '%qual%' THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(mo.vertriebsphase) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(mo.vertriebsphase) LIKE '%id. decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(mo.vertriebsphase) LIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN LOWER(mo.vertriebsphase) LIKE '%proposal%' OR LOWER(mo.vertriebsphase) LIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(mo.vertriebsphase) LIKE '%negotiation%' OR LOWER(mo.vertriebsphase) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(mo.vertriebsphase) LIKE '%closed won%' OR LOWER(mo.vertriebsphase) LIKE '%gewonnen%' THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) LIKE '%closed lost%' OR LOWER(mo.vertriebsphase) LIKE '%verloren%' OR LOWER(mo.vertriebsphase) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(mo.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(mo.zieldatum, 'M/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL CloseDate
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN mo.auftragswert IS NULL THEN NULL
            WHEN mo.auftragswert ~ '^\s*$' THEN NULL -- Handle empty strings
            ELSE
                REGEXP_REPLACE(
                    CASE
                        WHEN mo.auftragswert LIKE '%.%,%' AND STRPOS(mo.auftragswert, ',') > STRPOS(mo.auftragswert, '.') THEN REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.')
                        WHEN mo.auftragswert LIKE '%,' AND STRPOS(mo.auftragswert, '.') = 0 THEN REPLACE(mo.auftragswert, ',', '.')
                        ELSE mo.auftragswert
                    END,
                '[^\d\.\-]', '', 'g')
        END AS DOUBLE PRECISION
    ) AS "Amount",
    UPPER(
        CASE
            WHEN LOWER(mo.waehrungscode) IN ('chf', 'swiss franc') THEN 'CHF'
            WHEN LOWER(mo.waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
            WHEN LOWER(mo.waehrungscode) IN ('usd', 'dollar', '$') THEN 'USD'
            WHEN LOWER(mo.waehrungscode) IN ('gbp', 'pound', '£') THEN 'GBP'
            ELSE NULL
        END
    ) AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
ON
    mo.kunden_ref = mk.kundennummer