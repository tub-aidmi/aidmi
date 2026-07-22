{{ config(materialized='table') }}

SELECT
    MD5(TRIM(opp_kennung)) AS "Id",
    COALESCE(TRIM(titel), TRIM(opp_kennung)) AS "Name",
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
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(zieldatum, 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')
            WHEN zieldatum ~ '^\d{8}$' THEN TO_DATE(zieldatum, 'YYYYMMDD')
            WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(zieldatum, 'MM/DD/YYYY')
            ELSE NULL
        END,
        CURRENT_DATE
    )::TEXT AS "CloseDate", -- Cast back to TEXT to match target spec
    CASE
        WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE
            -- Pre-process the string to remove any leading non-numeric, non-separator characters
            -- (e.g., currency symbols like "EUR ") before evaluating format.
            CASE
                -- European format (e.g., 1.234,56)
                WHEN REGEXP_REPLACE(TRIM(auftragswert), '^[^0-9\.,]+', '') ~ '^[0-9]{1,3}(\.[0-9]{3})*,[0-9]{2}$' THEN
                    REPLACE(REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '^[^0-9\.,]+', ''), '.', ''), ',', '.')::DOUBLE PRECISION
                -- US format (e.g., 1,234.56) or simple number (no thousand separators)
                WHEN REGEXP_REPLACE(TRIM(auftragswert), '^[^0-9\.,]+', '') ~ '^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$' OR REGEXP_REPLACE(TRIM(auftragswert), '^[^0-9\.,]+', '') ~ '^[0-9]+(\.[0-9]+)?$' THEN
                    REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '^[^0-9\.,]+', ''), ',', '')::DOUBLE PRECISION
                ELSE NULL
            END
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    MD5(TRIM(kunden_ref)) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }}
