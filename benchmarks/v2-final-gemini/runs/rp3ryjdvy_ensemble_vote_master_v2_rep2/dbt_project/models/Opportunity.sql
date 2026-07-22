{{ config(materialized='table') }}

SELECT
    o.opp_kennung AS "Id",
    TRIM(COALESCE(o.titel, 'Opportunity ' || o.opp_kennung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('prospecting', 'anbahnung') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('id. decision makers', 'entscheider identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('proposal/price quote', 'angebot/preiskalkulation') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(o.zieldatum AS DATE), 'YYYY-MM-DD') -- YYYY-MM-DD
            WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' THEN NULL
        -- European format: 1.234,56 -> 1234.56
        WHEN o.auftragswert ~ '^\d{1,3}(\.\d{3})*,\d{1,2}$' THEN
            CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        -- American format: 1,234.56 -> 1234.56
        WHEN o.auftragswert ~ '^\d{1,3}(,\d{3})*\.\d{1,2}$' THEN
            CAST(REPLACE(o.auftragswert, ',', '') AS DOUBLE PRECISION)
        -- Just digits and optional decimal point: 1234.56 or 1234
        WHEN o.auftragswert ~ '^\d+(\.\d+)?$' THEN
            CAST(o.auftragswert AS DOUBLE PRECISION)
        -- Just digits and optional comma as decimal: 1234,56
        WHEN o.auftragswert ~ '^\d+(,\d+)?$' THEN
            CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(o.waehrungscode) AS "CurrencyIsoCode",
    MD5(k.kundennummer) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON o.kunden_ref = k.kundennummer
