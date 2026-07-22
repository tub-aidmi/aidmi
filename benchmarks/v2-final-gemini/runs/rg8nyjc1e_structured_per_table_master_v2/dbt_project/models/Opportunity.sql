-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    o.opp_kennung AS "Id",
    o.titel AS "Name",
    CASE
        WHEN LOWER(o.vertriebsphase) IN ('in kontakt', 'prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(o.vertriebsphase) IN ('quali', 'qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(o.vertriebsphase) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'won') THEN 'Closed Won'
        WHEN LOWER(o.vertriebsphase) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default stage for unmapped values as it is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(o.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback if all parsing fails, as CloseDate is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN TRIM(LOWER(o.auftragswert)) ~ '^none$' OR o.auftragswert IS NULL THEN NULL
        ELSE CAST(REGEXP_REPLACE(o.auftragswert, '[^0-9.-]+', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(o.waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(o.waehrungscode) IN ('chf', 'frank', 'franken') THEN 'CHF'
        WHEN LOWER(o.waehrungscode) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(o.waehrungscode) = 'gbp' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    o.kunden_ref AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o