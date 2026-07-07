{{ config(materialized='table') }}

SELECT
    MD5(mo.opp_kennung) AS "Id",
    COALESCE(mo.titel, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) LIKE '%qualification%' THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(mo.vertriebsphase) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(mo.vertriebsphase) LIKE '%decision maker%' THEN 'Id. Decision Makers'
        WHEN LOWER(mo.vertriebsphase) LIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN LOWER(mo.vertriebsphase) LIKE '%proposal%' OR LOWER(mo.vertriebsphase) LIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(mo.vertriebsphase) LIKE '%negotiation%' OR LOWER(mo.vertriebsphase) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(mo.vertriebsphase) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' for NOT NULL target column
    END AS "StageName",
    CASE
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(NOW(), 'YYYY-MM-DD') -- Fallback for NOT NULL target column
    END AS "CloseDate",
    CASE
        WHEN mo.auftragswert IS NULL OR TRIM(mo.auftragswert) = '' THEN NULL
        ELSE
            CAST(
                NULLIF(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                TRIM(mo.auftragswert),
                                '[^0-9,.]+', '', 'g' -- Remove currency symbols and other non-numeric, non-comma, non-dot chars
                            ),
                            -- Heuristic for European thousand separators: if a comma is present, assume dots are thousand separators and remove them.
                            -- Otherwise, leave dots alone (assuming they are decimal points for American format, or no dots at all).
                            CASE WHEN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^,]+', '', 'g') LIKE '%,%' THEN '\.' ELSE '(?!)' END,
                            '', 'g'
                        ),
                        ',', '.' -- Replace decimal comma with dot
                    ),
                    ''
                )
            AS DOUBLE PRECISION)
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    MD5(mk.kundennummer) AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} mk
ON
    mo.kunden_ref = mk.kundennummer
