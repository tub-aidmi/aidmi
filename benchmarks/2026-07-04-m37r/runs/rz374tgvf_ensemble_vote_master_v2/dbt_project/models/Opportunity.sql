{{ config(materialized='table') }}

WITH cleaned_amounts AS (
    SELECT
        mo.opp_kennung,
        mo.titel,
        mo.vertriebsphase,
        mo.zieldatum,
        mo.waehrungscode,
        mo.kunden_ref,
        mo.auftragswert,
        mk.kundennummer,
        -- Step 1: Normalize NULLs and clean currency symbols/whitespace
        TRIM(
            REGEXP_REPLACE(
                LOWER(COALESCE(mo.auftragswert, '')),
                '(eur|chf|usd|gbp|€|\[dollar_sign]|euro|dollar)\s*|\s*(eur|chf|usd|gbp|€|\[dollar_sign]|euro|dollar)',
                '',
                'g'
            )
        ) AS pre_cleaned_amount_str
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON
        mo.kunden_ref = mk.kundennummer
)
SELECT
    ca.opp_kennung AS "Id",
    COALESCE(ca.titel, 'Unknown Opportunity ' || ca.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(ca.vertriebsphase) IN ('won', 'closed won') THEN 'Closed Won'
        WHEN LOWER(ca.vertriebsphase) IN ('lost', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(ca.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(ca.vertriebsphase) IN ('prospecting', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(ca.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL target StageName
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(ca.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ca.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ca.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ca.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN ca.pre_cleaned_amount_str IS NULL OR ca.pre_cleaned_amount_str = '' THEN NULL
        ELSE
            CAST(
                CASE
                    -- European dot-comma format: 1.234,56 (dot as thousand, comma as decimal)
                    WHEN POSITION(',' IN ca.pre_cleaned_amount_str) > 0
                        AND POSITION('.' IN ca.pre_cleaned_amount_str) > 0
                        AND POSITION(',' IN ca.pre_cleaned_amount_str) > POSITION('.' IN ca.pre_cleaned_amount_str)
                    THEN
                        REPLACE(REPLACE(ca.pre_cleaned_amount_str, '.', ''), ',' , '.')
                    -- US comma-dot format: 1,234.56 (comma as thousand, dot as decimal)
                    WHEN POSITION(',' IN ca.pre_cleaned_amount_str) > 0
                        AND POSITION('.' IN ca.pre_cleaned_amount_str) > 0
                        AND POSITION('.' IN ca.pre_cleaned_amount_str) > POSITION(',' IN ca.pre_cleaned_amount_str)
                    THEN
                        REPLACE(ca.pre_cleaned_amount_str, ',' , '')
                    -- Only comma present: 123,45 (assume European decimal)
                    WHEN POSITION(',' IN ca.pre_cleaned_amount_str) > 0
                    THEN
                        REPLACE(ca.pre_cleaned_amount_str, ',' , '.')
                    ELSE
                        -- Only dot present, or no separators: assume standard decimal
                        ca.pre_cleaned_amount_str
                END AS DOUBLE PRECISION
            )
    END AS "Amount",
    UPPER(ca.waehrungscode) AS "CurrencyIsoCode",
    ca.kundennummer AS "AccountId",
    ca.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_amounts AS ca
