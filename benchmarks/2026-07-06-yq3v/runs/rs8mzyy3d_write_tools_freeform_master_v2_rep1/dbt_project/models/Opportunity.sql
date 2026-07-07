-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    MD5(TRIM(opp_kennung)) AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('won', 'closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'closed won') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SUBSTRING(zieldatum FROM 1 FOR POSITION('/' IN zieldatum) - 1)::INT <= 12 AND SUBSTRING(SUBSTRING(zieldatum FROM POSITION('/' IN zieldatum) + 1) FROM 1 FOR POSITION('/' IN SUBSTRING(zieldatum FROM POSITION('/' IN zieldatum) + 1)) - 1)::INT <= 31 THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE
            CAST(REPLACE(
                REPLACE(
                    REGEXP_REPLACE(auftragswert, '[^0-9,\-.]', '', 'g'), -- Remove non-numeric except comma, dot, hyphen
                    '.', '', 'g' -- Remove thousand separators (dots)
                ),
                ',', '.' -- Replace comma with dot for decimal
            ) AS DOUBLE PRECISION)
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    MD5(TRIM(kunden_ref)) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }}
WHERE
    opp_kennung IS NOT NULL;
