
-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(mo.titel, 'Untitled Opportunity') AS "Name",
    CASE LOWER(TRIM(mo.vertriebsphase))
        WHEN 'won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'

        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'

        WHEN 'in prüfung' THEN 'Negotiation/Review'

        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualification' THEN 'Qualification'

        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'in kontakt' THEN 'Prospecting'

        ELSE 'Prospecting' -- Fallback for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
            WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default value for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN TRIM(mo.auftragswert) IS NULL OR TRIM(mo.auftragswert) = '' THEN NULL
        ELSE
            CAST(
                CASE
                    WHEN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g') = '' THEN NULL -- If cleanup results in an empty string, it's not a number
                    ELSE
                        CASE
                            WHEN POSITION(',' IN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g')) > 0
                                 AND POSITION('.' IN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g')) > 0
                                 AND POSITION(',' IN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g')) > POSITION('.' IN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g')) THEN
                                -- European format: remove thousand-separator dots, replace decimal comma with dot
                                REPLACE(REPLACE(REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g'), '.', ''), ',', '.')
                            ELSE
                                -- American format or only one/no separator: remove thousand-separator commas
                                REPLACE(REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9.,-]+', '', 'g'), ',', '')
                        END
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    CASE LOWER(TRIM(mo.waehrungscode))
        WHEN 'dollar' THEN 'USD'
        WHEN '€' THEN 'EUR'
        WHEN 'eur' THEN 'EUR'
        ELSE UPPER(TRIM(mo.waehrungscode))
    END AS "CurrencyIsoCode",
    mo.kunden_ref AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_opportunities') }} AS mo
