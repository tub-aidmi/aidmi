{{
    config(materialized='table')
}}

SELECT
    MD5(opp.opp_kennung) AS "Id",
    COALESCE(TRIM(opp.titel), 'No Opportunity Title') AS "Name",
    CASE
        WHEN opp.vertriebsphase = 'Angebot erstellt' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase = 'Verhandlung' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase = 'Abgeschlossen gewonnen' THEN 'Closed Won'
        WHEN opp.vertriebsphase = 'Abgeschlossen verloren' THEN 'Closed Lost'
        WHEN opp.vertriebsphase = 'Qualifizierung' THEN 'Qualification'
        WHEN opp.vertriebsphase = 'Bedürfnissanalyse' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase = 'Wertangebot' THEN 'Value Proposition'
        WHEN opp.vertriebsphase = 'Identifizierung von Entscheidungsträgern' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase = 'Interesse' THEN 'Prospecting' -- Assuming 'Interesse' maps to Prospecting
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(opp.zieldatum::DATE, 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default for NOT NULL
    END AS "CloseDate",
    COALESCE(
        CAST(
            CASE
                WHEN opp.auftragswert IS NULL OR TRIM(opp.auftragswert) = '' THEN '0'
                ELSE
                    -- First, clean any non-numeric, non-comma, non-dot, non-hyphen character
                    CASE
                        -- Check for explicit European format (dot for thousands, comma for decimal)
                        WHEN REGEXP_REPLACE(opp.auftragswert, '[^0-9,\.-]', '', 'g') ~ '^-?\d{1,3}(\.\d{3})+,\d+$' THEN
                            REPLACE(REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^0-9,\.-]', '', 'g'), '.', '', 'g'), ',', '.')
                        -- Check for simple European format (only comma for decimal)
                        WHEN REGEXP_REPLACE(opp.auftragswert, '[^0-9,\.-]', '', 'g') ~ '^-?\d+,\d+$' THEN
                            REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^0-9,\.-]', '', 'g'), ',', '.')
                        -- Assume US/standard format (comma for thousands, dot for decimal)
                        ELSE
                            REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^0-9,\.-]', '', 'g'), ',', '', 'g')
                    END
            END AS TEXT
        ) AS DOUBLE PRECISION
        , 0.0
    ) AS "Amount",
    TRIM(UPPER(opp.waehrungscode)) AS "CurrencyIsoCode",
    MD5(opp.kunden_ref) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
