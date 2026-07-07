-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(mo.titel, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) IN ('closed won', 'won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closed_won') THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(mo.vertriebsphase) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(mo.vertriebsphase) IN ('id. decision makers', 'entscheidungsträger identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(mo.vertriebsphase) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(mo.vertriebsphase) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(mo.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        WHEN LOWER(mo.vertriebsphase) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)', 'closed_lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting for unmapped values, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum -- Already YYYY-MM-DD
            WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL -- Prefer NULL for unparseable dates before COALESCE
        END,
        '1900-01-01' -- Default for NOT NULL target if unparseable, per transformation guidelines
    ) AS "CloseDate",
    CAST(
        NULLIF(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(mo.auftragswert, '[^0-9,\.-]+', '', 'g'), -- Remove currency symbols and non-numeric chars
                    '\.(?=\d{3,})', '', 'g' -- Remove thousand separators (dots followed by 3+ digits)
                ),
                ',', '.', 'g' -- Replace comma with dot for decimal separator
            ),
        '' -- If the result is an empty string, make it NULL
        ) AS DOUBLE PRECISION
    ) AS "Amount",
    CASE
        WHEN LOWER(mo.waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(mo.waehrungscode) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(mo.waehrungscode) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(mo.waehrungscode) IN ('chf') THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    mo.kunden_ref AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo