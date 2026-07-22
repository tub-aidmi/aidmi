-- depends_on: {{ ref('account') }}
{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(mo.titel, 'Opportunity ' || mo.opp_kennung) AS "Name",
    CASE
        WHEN mo.vertriebsphase ILIKE 'anbahnung' THEN 'Prospecting'
        WHEN mo.vertriebsphase ILIKE 'qualifizierung' THEN 'Qualification'
        WHEN mo.vertriebsphase ILIKE 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN mo.vertriebsphase ILIKE 'wertangebot' THEN 'Value Proposition'
        WHEN mo.vertriebsphase ILIKE 'id. entscheidungsträger' THEN 'Id. Decision Makers'
        WHEN mo.vertriebsphase ILIKE 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN mo.vertriebsphase ILIKE 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN mo.vertriebsphase ILIKE 'verhandlung/prüfung' THEN 'Negotiation/Review'
        WHEN mo.vertriebsphase ILIKE 'abgeschlossen gewonnen' THEN 'Closed Won'
        WHEN mo.vertriebsphase ILIKE 'abgeschlossen verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(mo.zieldatum, 'YYYY-MM-DD')
                WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(mo.zieldatum, 'DD.MM.YYYY')
                WHEN mo.zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_DATE(mo.zieldatum, 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        '1900-01-01' -- Default for NOT NULL CloseDate if parsing fails or source is NULL
    ) AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.'), '[^0-9.]', '', 'g') ~ '^-?\d+(\.\d+)?$'
        THEN CAST(REGEXP_REPLACE(REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.'), '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON mo.kunden_ref = mk.kundennummer