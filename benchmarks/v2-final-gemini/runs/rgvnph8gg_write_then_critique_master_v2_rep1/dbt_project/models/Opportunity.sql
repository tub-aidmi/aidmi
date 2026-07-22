{{ config(materialized='table') }}

SELECT
    MD5(o.opp_kennung) AS "Id",
    COALESCE(TRIM(o.titel), 'Unknown Opportunity') AS "Name",
    CASE TRIM(LOWER(o.vertriebsphase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL column
    END AS "StageName",
    CASE
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Meaningful default for NOT NULL date
    END AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL THEN NULL
        WHEN o.auftragswert ~ '^\s*[A-Z]{3}\s*\d{1,3}(\.\d{3})*,\d+\s*$' THEN CAST(REPLACE(REPLACE(REGEXP_REPLACE(o.auftragswert, '^\s*[A-Z]{3}\s*|', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^\s*\d{1,3}(\.\d{3})*,\d+\s*$' THEN CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^\s*[A-Z]{3}\s*\d+(\.\d+)?\s*$' THEN CAST(REGEXP_REPLACE(o.auftragswert, '^\s*[A-Z]{3}\s*|', '', 'g') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^\s*\d+(\.\d+)?\s*$' THEN CAST(o.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    o.waehrungscode AS "CurrencyIsoCode",
    MD5(TRIM(k.kundennummer)) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
ON
    o.kunden_ref = k.kundennummer