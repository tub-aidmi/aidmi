
{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        opp.opp_kennung,
        opp.titel,
        opp.vertriebsphase,
        opp.zieldatum,
        opp.waehrungscode,
        opp.kunden_ref,
        TRIM(REGEXP_REPLACE(LOWER(opp.auftragswert), '(eur |€)', '', 'g')) AS cleaned_amount_str
    FROM
        {{ source('fixture_master_src', 'master_opportunities') }} AS opp
)
SELECT
    co.opp_kennung AS "Id",
    COALESCE(TRIM(co.titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(co.vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(co.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(co.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(co.vertriebsphase) IN ('prospecting', 'in kontakt', 'prospect') THEN 'Prospecting'
        WHEN LOWER(co.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    TO_CHAR(
        COALESCE(
            CASE
                WHEN co.zieldatum = '0000-00-00' THEN NULL -- Handle specific invalid date format
                WHEN co.zieldatum ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$' THEN CAST(co.zieldatum AS DATE)
                WHEN co.zieldatum ~ '^(0[1-9]|[12]\d|3[01])\.(0[1-9]|1[0-2])\.\d{4}$' THEN TO_DATE(co.zieldatum, 'DD.MM.YYYY')
                WHEN co.zieldatum ~ '^(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])\/\d{4}$' THEN TO_DATE(co.zieldatum, 'MM/DD/YYYY')
                WHEN co.zieldatum ~ '^\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$' THEN TO_DATE(co.zieldatum, 'YYYYMMDD')
                ELSE NULL
            END,
            '2099-12-31'::DATE -- Default for NOT NULL target column
        ),
        'YYYY-MM-DD'
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN co.cleaned_amount_str IS NULL OR co.cleaned_amount_str = '' THEN NULL
            -- If it contains both comma and dot
            WHEN co.cleaned_amount_str ~ ',' AND co.cleaned_amount_str ~ '\\.' THEN
                CASE
                    -- European format: dot thousand separator, comma decimal separator (e.g., 1.234.567,89)
                    WHEN POSITION('.' IN co.cleaned_amount_str) < POSITION(',' IN co.cleaned_amount_str) THEN
                        REGEXP_REPLACE(REGEXP_REPLACE(co.cleaned_amount_str, '\\.', '', 'g'), ',', '.', 'g')
                    -- US format: comma thousand separator, dot decimal separator (e.g., 1,234,567.89)
                    WHEN POSITION(',' IN co.cleaned_amount_str) < POSITION('.' IN co.cleaned_amount_str) THEN
                        REGEXP_REPLACE(co.cleaned_amount_str, ',', '', 'g')
                    ELSE NULL -- Ambiguous or malformed with both separators in an invalid order
                END
            -- If it contains only a comma (must be European decimal separator)
            WHEN co.cleaned_amount_str ~ ',' THEN
                REGEXP_REPLACE(co.cleaned_amount_str, ',', '.', 'g')
            -- If it contains only a dot (must be US decimal separator, or integer)
            WHEN co.cleaned_amount_str ~ '\\.' THEN
                -- Check for multiple dots, which would be malformed in a US-style number
                CASE
                    WHEN (LENGTH(co.cleaned_amount_str) - LENGTH(REGEXP_REPLACE(co.cleaned_amount_str, '\\.', '', 'g'))) <= 1 THEN co.cleaned_amount_str
                    ELSE NULL -- Multiple dots, no comma, malformed
                END
            -- If it contains neither comma nor dot (must be an integer)
            WHEN co.cleaned_amount_str ~ '^-?\d+$' THEN
                co.cleaned_amount_str
            ELSE NULL -- Catch all for unparseable strings
        END
    AS DOUBLE PRECISION) AS "Amount",
    co.waehrungscode AS "CurrencyIsoCode",
    kunden.kundennummer AS "AccountId",
    co.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS co
LEFT JOIN
    {{ source('fixture_master_src', 'master_kunden') }} AS kunden
    ON co.kunden_ref = kunden.kundennummer
