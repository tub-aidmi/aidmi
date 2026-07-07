{{
    config(materialized='table')
}}

SELECT
    o.opp_kennung AS "Id",
    COALESCE(TRIM(o.titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL target enum
    END AS "StageName",
    CASE
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default for NOT NULL target column
    END AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' OR TRIM(o.auftragswert) ILIKE 'None' THEN NULL
        ELSE
            CAST(
                CASE
                    -- Check if it contains a comma, implying European format (e.g., 1.234,56)
                    WHEN POSITION(',' IN TRIM(REGEXP_REPLACE(o.auftragswert, '[^0-9.,-]+', '', 'g'))) > 0 THEN
                        REPLACE(REPLACE(TRIM(REGEXP_REPLACE(o.auftragswert, '[^0-9.,-]+', '', 'g')), '.', ''), ',', '.')
                    -- Otherwise assume US format or simple number (e.g., 1,234.56 or 1234.56)
                    ELSE
                        TRIM(REGEXP_REPLACE(o.auftragswert, '[^0-9.-]+', '', 'g'))
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    UPPER(TRIM(o.waehrungscode)) AS "CurrencyIsoCode",
    o.kunden_ref AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o