{{ config(materialized='table') }}

SELECT
    MD5(mo.opp_kennung) AS "Id",
    COALESCE(mo.titel, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) IN ('qualification', 'qualifikation', 'quali', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmapped values, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default date string if all parsing fails or zieldatum is null
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN mo.auftragswert IS NULL THEN NULL
            ELSE
                CASE
                    -- European format: dot as thousands, comma as decimal (e.g., 1.234,56)
                    WHEN mo.auftragswert LIKE '%.%' AND mo.auftragswert LIKE '%,%'
                         AND POSITION(RTRIM(mo.auftragswert, '-') IN mo.auftragswert) > POSITION('.' IN RTRIM(mo.auftragswert, '-')) THEN
                        REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.')
                    -- European format: only comma as decimal (e.g., 123,45)
                    WHEN mo.auftragswert LIKE '%,%' AND mo.auftragswert NOT LIKE '%.%' THEN
                        REPLACE(mo.auftragswert, ',', '.')
                    -- American format or standard number: clean and cast
                    ELSE
                        REGEXP_REPLACE(
                            LOWER(mo.auftragswert),
                            '^(eur |usd |£|€|chf )|[^0-9\.\-]+',
                            '',
                            'g'
                        )
                END
        END
    AS DOUBLE PRECISION) AS "Amount",
    CASE
        WHEN LOWER(mo.waehrungscode) IN ('euro', 'eur', '€') THEN 'EUR'
        WHEN LOWER(mo.waehrungscode) IN ('usd', 'dollar') THEN 'USD'
        WHEN LOWER(mo.waehrungscode) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(mo.waehrungscode) IN ('chf') THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(mo.kunden_ref) AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
