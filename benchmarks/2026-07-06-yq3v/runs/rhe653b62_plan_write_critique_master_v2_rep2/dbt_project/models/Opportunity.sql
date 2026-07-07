{{ config(materialized='table') }}

SELECT
    MD5(o.opp_kennung) AS "Id",
    o.titel AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(o.vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closed-won') THEN 'Closed Won'
            WHEN LOWER(o.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)', 'closed-lost') THEN 'Closed Lost'
            WHEN LOWER(o.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
            WHEN LOWER(o.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
            WHEN LOWER(o.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
            ELSE 'Prospecting' -- Default for NOT NULL StageName
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        CASE
            WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
            WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE '1900-01-01' -- Default for NOT NULL CloseDate
        END,
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL THEN NULL
        ELSE
            (
                SELECT
                    CASE
                        -- Check if the final cleaned string is a valid number before casting
                        WHEN cleaned_num_str ~ '^[+\-]?(\d+(\.\d*)?|\.\d+)$' THEN CAST(cleaned_num_str AS DOUBLE PRECISION)
                        ELSE NULL
                    END
                FROM (
                    SELECT
                        CASE
                            -- European format: 1.234,56 -> 1234.56
                            WHEN normalized_val ~ '^\d+\.\d+,\d+$' THEN REPLACE(REPLACE(normalized_val, '.'), ',', '.')
                            -- European format: 123,45 -> 123.45
                            WHEN normalized_val ~ '^\d+,\d+$' THEN REPLACE(normalized_val, ',', '.')
                            -- Default to American/plain (1234.56 or 1234)
                            ELSE normalized_val
                        END AS cleaned_num_str
                    FROM (
                        -- Remove common currency prefixes/suffixes and extra spaces
                        SELECT
                            TRIM(
                                REGEXP_REPLACE(
                                    LOWER(o.auftragswert),
                                    '^(eur|gbp|usd|chf|€|£|$)\s*|\s*(eur|gbp|usd|chf|€|£|$)$',
                                    '',
                                    'g'
                                )
                            ) AS normalized_val
                    ) AS _s1
                ) AS _s2
            )
    END AS "Amount",
    COALESCE(
        CASE
            WHEN LOWER(o.waehrungscode) IN ('usd', 'dollar') THEN 'USD'
            WHEN LOWER(o.waehrungscode) IN ('gbp', '£') THEN 'GBP'
            WHEN LOWER(o.waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
            WHEN LOWER(o.waehrungscode) IN ('chf') THEN 'CHF'
            ELSE 'EUR' -- Default for NOT NULL CurrencyIsoCode
        END,
        'EUR'
    ) AS "CurrencyIsoCode",
    MD5(o.kunden_ref) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
'''