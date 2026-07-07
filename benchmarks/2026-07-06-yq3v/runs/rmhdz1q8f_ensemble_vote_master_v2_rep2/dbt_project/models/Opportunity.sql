-- depends_on: {{ ref('account') }}

{{ config(materialized='table') }}

WITH opportunities_raw AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
)
SELECT
    o.opp_kennung AS "Id",
    COALESCE(o.titel, o.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(o.vertriebsphase) IN ('won', 'gewonnen', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(o.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(o.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(o.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(o.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        -- Add more mappings as needed
        ELSE 'Prospecting' -- Default value for NOT NULL column
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(o.zieldatum, 'YYYY-MM-DD')
                WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.zieldatum, 'DD.MM.YYYY')
                WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(o.zieldatum, 'MM/DD/YYYY')
                WHEN o.zieldatum ~ '^\d{8}$' THEN TO_DATE(o.zieldatum, 'YYYYMMDD')
                ELSE NULL
            END, 'YYYY-MM-DD'
        ), '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' THEN NULL
        ELSE
            CAST(
                REPLACE(
                    REPLACE(
                        TRIM(REGEXP_REPLACE(o.auftragswert, '[^0-9.,-]', '', 'g')),
                        '.', '' -- Remove thousand separators (dots)
                    ),
                    ',', '.' -- Replace decimal comma with dot
                ) AS DOUBLE PRECISION
            )
    END AS "Amount",
    o.waehrungscode AS "CurrencyIsoCode",
    k.kundennummer AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities_raw o
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} k
ON
    o.kunden_ref = k.kundennummer
