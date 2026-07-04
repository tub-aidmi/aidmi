-- This dbt model transforms raw opportunity data into the target Opportunity schema.
-- It handles ID generation, enum mapping, date parsing, and amount normalization.

{{ config(materialized='table') }}

WITH cleaned_data AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.vertriebsphase,
        o.zieldatum,
        o.auftragswert,
        o.waehrungscode,
        o.kunden_ref,
        k.kundennummer AS account_kundennummer,
        TRIM(REGEXP_REPLACE(o.auftragswert, '[^0-9\.,]+', '', 'g')) AS cleaned_base_amount_str
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
        ON o.kunden_ref = k.kundennummer
)
SELECT
    SUBSTRING(MD5(cd.opp_kennung) FOR 18)::TEXT AS "Id",
    COALESCE(INITCAP(TRIM(cd.titel)), 'Unnamed Opportunity') AS "Name",
    CASE UPPER(TRIM(cd.vertriebsphase))
        WHEN 'PROSPEKTIERUNG' THEN 'Prospecting'
        WHEN 'QUALIFIKATION' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'WERTVERSPRECHEN' THEN 'Value Proposition'
        WHEN 'ENT. ENTSCHEIDER' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PREIS' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for StageName as it is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN cd.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(cd.zieldatum, 'DD.MM.YYYY')
                WHEN cd.zieldatum ~ '^\d{8}$' THEN TO_DATE(cd.zieldatum, 'YYYYMMDD')
                WHEN cd.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(cd.zieldatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for CloseDate as it is NOT NULL
    ) AS "CloseDate",
    NULLIF(
        CASE
            WHEN cd.cleaned_base_amount_str IS NULL OR cd.cleaned_base_amount_str = '' THEN NULL
            WHEN cd.cleaned_base_amount_str LIKE '%.%' AND cd.cleaned_base_amount_str LIKE '%,%' THEN -- Contains both dot and comma
                CASE
                    WHEN POSITION(''',''' IN cd.cleaned_base_amount_str) > POSITION('.' IN cd.cleaned_base_amount_str) THEN -- Comma is the last separator (e.g., 1.234,56)
                        REPLACE(REPLACE(cd.cleaned_base_amount_str, '.', ''), ',' , '.') -- Remove dots, then replace comma with dot
                    ELSE -- Dot is the last separator (e.g., 1,234.56)
                        REPLACE(cd.cleaned_base_amount_str, ',' , '') -- Remove commas
                END
            WHEN cd.cleaned_base_amount_str LIKE '%,%' THEN -- Only comma (e.g., 123,45) - treat as European decimal
                REPLACE(cd.cleaned_base_amount_str, ',' , '.')
            WHEN cd.cleaned_base_amount_str LIKE '%.%' THEN -- Only dot (e.g., 123.45) - treat as US decimal
                cd.cleaned_base_amount_str
            ELSE -- No separators, just digits
                cd.cleaned_base_amount_str
        END,
    '')::DOUBLE PRECISION AS "Amount",
    UPPER(TRIM(cd.waehrungscode)) AS "CurrencyIsoCode",
    CASE
        WHEN cd.account_kundennummer IS NOT NULL THEN SUBSTRING(MD5(cd.account_kundennummer) FOR 18)::TEXT
        ELSE NULL
    END AS "AccountId",
    cd.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM cleaned_data AS cd