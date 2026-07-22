-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(mo.titel, mo.opp_kennung) AS "Name",
    CASE
        WHEN mo.vertriebsphase = 'Angebot' THEN 'Proposal/Price Quote'
        WHEN mo.vertriebsphase = 'Verhandlung' THEN 'Negotiation/Review'
        WHEN mo.vertriebsphase = 'Gewonnen' THEN 'Closed Won'
        WHEN mo.vertriebsphase = 'Verloren' THEN 'Closed Lost'
        WHEN mo.vertriebsphase IS NULL THEN 'Prospecting' -- Default for NULL
        ELSE 'Prospecting' -- Default for unmapped values
    END AS "StageName",
    COALESCE(
        CASE
            WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CASE
        WHEN mo.auftragswert ~ '^\d+\.\d{3},\d+$' THEN -- European format with dot as thousand separator, comma as decimal
            CAST(REPLACE(REPLACE(mo.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^\d+,\d+$' THEN -- European format with comma as decimal
            CAST(REPLACE(mo.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^\d+(\.\d+)?$' THEN -- Standard dot decimal
            CAST(mo.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    mo.kunden_ref AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NOW() AS "CreatedDate",
    NOW() AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo