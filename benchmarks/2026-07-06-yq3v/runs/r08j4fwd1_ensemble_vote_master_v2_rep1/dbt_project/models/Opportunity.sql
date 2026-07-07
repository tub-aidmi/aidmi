{{ config(materialized='table') }}

SELECT
    o.opp_kennung AS "Id",
    COALESCE(o.titel, o.opp_kennung) AS "Name",
    CASE
        WHEN o.vertriebsphase ILIKE 'Interesse' THEN 'Prospecting'
        WHEN o.vertriebsphase ILIKE 'Qualifikation' THEN 'Qualification'
        WHEN o.vertriebsphase ILIKE 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN o.vertriebsphase ILIKE 'Wertversprechen' THEN 'Value Proposition'
        WHEN o.vertriebsphase ILIKE 'Entscheider identifizieren' THEN 'Id. Decision Makers'
        WHEN o.vertriebsphase ILIKE 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN o.vertriebsphase ILIKE 'Angebot/Preis' THEN 'Proposal/Price Quote'
        WHEN o.vertriebsphase ILIKE 'Verhandlung/Überprüfung' THEN 'Negotiation/Review'
        WHEN o.vertriebsphase ILIKE 'Gewonnen' THEN 'Closed Won'
        WHEN o.vertriebsphase ILIKE 'Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    CASE
        WHEN o.zieldatum IS NULL OR TRIM(o.zieldatum) = '' THEN CURRENT_DATE::TEXT
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum -- Already YYYY-MM-DD
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE CURRENT_DATE::TEXT -- Fallback for unparseable dates, as target is NOT NULL
    END AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' THEN NULL
        ELSE
            -- First remove all non-numeric characters except comma and dot
            -- and then apply the logic for European vs US format
            CAST(
                CASE
                    WHEN POSITION(',' IN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g')) > 0
                        AND POSITION('.' IN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g')) > 0 THEN
                        -- Both comma and dot present. Check order.
                        CASE
                            WHEN POSITION('.' IN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g')) < POSITION(',' IN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g')) THEN
                                -- European style (e.g., 1.234,56). Remove dots, replace comma with dot.
                                REPLACE(REPLACE(REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g'), '.', ''), ',', '.')
                            ELSE
                                -- American style (e.g., 1,234.56). Remove commas.
                                REPLACE(REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g'), ',', '')
                        END
                    WHEN POSITION(',' IN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g')) > 0 THEN
                        -- Only comma present, assume European decimal (e.g., 1234,56). Replace comma with dot.
                        REPLACE(REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g'), ',', '.')
                    ELSE
                        -- Only dot present, or no separators, assume American decimal or integer (e.g., 1234.56 or 1234). Keep as is.
                        REGEXP_REPLACE(o.auftragswert, '[^0-9.]', '', 'g')
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    o.waehrungscode AS "CurrencyIsoCode",
    o.kunden_ref AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o