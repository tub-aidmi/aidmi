-- This model transforms raw opportunity data into the target Opportunity schema.
{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(opp.titel, opp.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL CloseDate
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN opp.auftragswert IS NULL THEN NULL
            ELSE
                (SELECT
                    CASE
                        WHEN cleaned_val = '' THEN NULL -- Added to handle empty string after cleanup
                        WHEN cleaned_val LIKE '%.%,%' AND STRPOS(cleaned_val, ',') > STRPOS(cleaned_val, '.') THEN -- 1.234,56
                            REPLACE(REPLACE(cleaned_val, '.', ''), ',', '.')
                        WHEN cleaned_val LIKE '% dissolution ' AND STRPOS(cleaned_val, '.') > STRPOS(cleaned_val, ',') THEN -- 1,234.56
                            REPLACE(cleaned_val, ',', '')
                        WHEN cleaned_val LIKE '%,%' THEN -- 123,45 (only comma, assume decimal)
                            REPLACE(cleaned_val, ',', '.')
                        ELSE cleaned_val -- 123.45 or 123
                    END
                FROM (SELECT REGEXP_REPLACE(opp.auftragswert, '[^0-9\.,\-]', '', 'g') AS cleaned_val) AS _s
                )
        END
    AS DOUBLE PRECISION) AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    kunden.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    SUBSTRING(opp.kunden_ref FROM 4) = SUBSTRING(kunden.kundennummer FROM 6)