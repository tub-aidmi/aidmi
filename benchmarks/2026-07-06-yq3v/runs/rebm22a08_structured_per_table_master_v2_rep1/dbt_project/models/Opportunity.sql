{{ config(materialized='table') }}

SELECT
    o.opp_kennung AS "Id",
    o.titel AS "Name",
    CASE
        WHEN LOWER(o.vertriebsphase) IN ('won', 'gewonnen', 'abgeschlossen (gewonnen)', 'closed won', 'closed won') THEN 'Closed Won'
        WHEN LOWER(o.vertriebsphase) IN ('lost', 'verloren', 'abgeschlossen (verloren)', 'closed lost', 'lost') THEN 'Closed Lost'
        WHEN LOWER(o.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(o.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(o.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(o.zieldatum AS DATE), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE TO_CHAR(CAST('1970-01-01' AS DATE), 'YYYY-MM-DD') -- Default for unparseable dates
        END,
        TO_CHAR(CAST('1970-01-01' AS DATE), 'YYYY-MM-DD') -- Fallback if COALESCE above evaluates to NULL for some reason
    ) AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' THEN NULL
        ELSE CAST(REPLACE(REPLACE(TRIM(REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g')), '.', ''), ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    o.waehrungscode AS "CurrencyIsoCode",
    k.kundennummer AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate", -- Placeholder for CreatedDate
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate", -- Placeholder for LastModifiedDate
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON o.kunden_ref = k.kundennummer
