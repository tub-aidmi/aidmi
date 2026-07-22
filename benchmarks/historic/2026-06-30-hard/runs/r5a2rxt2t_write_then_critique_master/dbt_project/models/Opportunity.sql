-- dbt model for Opportunity
{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(mo.titel, mo.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) IN ('won', 'closed won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(mo.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        CASE
            WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
            WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL target
    ) AS "CloseDate",
    CASE
        WHEN cleaned_amount ~ '^-?\d+(\.\d+)?$' AND cleaned_amount != ''
        THEN cleaned_amount::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    mo.kunden_ref AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_opportunities') }} AS mo
LEFT JOIN
    (SELECT
        opp_kennung,
        TRIM(REGEXP_REPLACE(
            REPLACE(REPLACE(LOWER(auftragswert), '.', ''), ',', '.'),
            '^(eur|usd|chf|€|dollar)\s*|[^0-9.\-]',
            '',
            'gi'
        )) AS cleaned_amount
    FROM
        {{ source('fixture_master_src', 'master_opportunities') }}
    ) AS cleaned_amounts
ON mo.opp_kennung = cleaned_amounts.opp_kennung