-- depends_on: {{ ref('account') }}
{{ config(materialized='table') }}

SELECT
    MD5(mo.opp_kennung) AS "Id",
    COALESCE(mo.titel, 'Opportunity ' || mo.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) LIKE '%in kontakt%' THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) LIKE '%qualification%' OR LOWER(mo.vertriebsphase) LIKE '%quali%' THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(mo.vertriebsphase) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(mo.vertriebsphase) LIKE '%id. decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(mo.vertriebsphase) LIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN LOWER(mo.vertriebsphase) LIKE '%proposal/price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(mo.vertriebsphase) LIKE '%negotiation/review%' THEN 'Negotiation/Review'
        WHEN LOWER(mo.vertriebsphase) LIKE '%closed won%' OR LOWER(mo.vertriebsphase) LIKE '%abgeschlossen (gewonnen)%' THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) LIKE '%closed lost%' OR LOWER(mo.vertriebsphase) LIKE '%abgeschlossen (verloren)%' OR LOWER(mo.vertriebsphase) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    TO_CHAR(
        CASE
            WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum::DATE
            WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(mo.zieldatum, 'DD.MM.YYYY')
            WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(mo.zieldatum, 'MM/DD/YYYY')
            WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_DATE(mo.zieldatum, 'YYYYMMDD')
            ELSE '1900-01-01'::DATE -- Default for NOT NULL target column if unparseable
        END,
        'YYYY-MM-DD'
    ) AS "CloseDate",
    CASE
        WHEN mo.auftragswert IS NULL OR TRIM(mo.auftragswert) = '' THEN NULL
        ELSE REGEXP_REPLACE(mo.auftragswert, '[^0-9\.-]', '', 'g')::DOUBLE PRECISION
    END AS "Amount",
    CASE UPPER(mo.waehrungscode)
        WHEN 'CHF' THEN 'CHF'
        WHEN 'CHFR' THEN 'CHF'
        WHEN 'CH' THEN 'CHF'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'EURO' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'USD' THEN 'USD'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(mk.kundennummer) AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON mo.kunden_ref = mk.kundennummer
