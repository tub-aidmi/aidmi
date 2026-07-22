{{ config(materialized='table') }}

SELECT
    MD5(TRIM(opp_kennung)) AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity ' || TRIM(opp_kennung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Fallback for unparseable/NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN TRIM(auftragswert) ~ '^\s*[+-]?(?:\d{1,3}(?:\.\d{3})*,\d+|(?:\d+(?:\.\d{2})?))$' THEN -- European format (e.g., 1.234,56 or 1234,56)
            CAST(REPLACE(REPLACE(TRIM(auftragswert), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(auftragswert) ~ '^\s*[+-]?(?:\d{1,3}(?:,\d{3})*\.\d+|\d+(?:\.\d+)?)$' THEN -- US/Standard format (e.g., 1,234.56 or 1234.56)
            CAST(REPLACE(TRIM(auftragswert), ',', '') AS DOUBLE PRECISION)
        WHEN TRIM(auftragswert) ~ '^\s*[+-]?\d+$' THEN -- Integer (e.g., 1234)
            CAST(TRIM(auftragswert) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    MD5(TRIM(kunden_ref)) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }}
