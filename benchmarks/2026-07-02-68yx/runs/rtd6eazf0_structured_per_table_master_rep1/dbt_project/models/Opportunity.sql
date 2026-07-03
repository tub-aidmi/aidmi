{{ config(materialized='table') }}

WITH raw_opps AS (
    SELECT *
    FROM {{ source('fixture_master_src', 'master_opportunities') }}
),
stage1 AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,

        UPPER(TRIM(COALESCE(vertriebsphase, ''))) AS phase_upper

    FROM raw_opps
),
stage2 AS (
    SELECT
        opp_kennung,
        titel,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        phase_upper,

        CASE
            WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' THEN NULL
            WHEN UPPER(TRIM(zieldatum)) = 'N/A' THEN NULL
            WHEN TRIM(zieldatum) = '0000-00-00' THEN NULL
            WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
            WHEN TRIM(zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
                TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN
                TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
                TO_CHAR(TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END AS close_date_raw,

        CASE
            WHEN TRIM(COALESCE(auftragswert, '')) = '' OR
                 UPPER(TRIM(COALESCE(auftragswert, ''))) IN ('NONE', 'NULL', '0') THEN NULL
            WHEN auftragswert IS NULL THEN NULL
            ELSE
                CASE
                    -- European format with dot as thousands sep and comma as decimal: e.g. "316.863,04"
                    WHEN REGEXP_REPLACE(auftragswert, '[^0-9,.€$£]', '', 'g') ~ '^\s*-?\d{1,3}(\.\d{3})+(,\d+)?$' THEN
                        CAST(
                            REPLACE(
                                REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9,.€$£\-]', ''), '\.', '', 'g'),
                                 ',', '.'
                             ) AS DOUBLE PRECISION
                         )
                    -- Simple comma decimal: e.g. "1234,56"
                    WHEN REGEXP_REPLACE(auftragswert, '[^0-9,.€$£]', '', 'g') ~ '^\s*-?\d+,\d+$' THEN
                        CAST(
                            REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9,.€$£\-]', ''), ',', '.') AS DOUBLE PRECISION
                        )
                    -- Standard decimal with dot: e.g. "1234.56"
                    WHEN REGEXP_REPLACE(auftragswert, '[^0-9,.€$£]', '', 'g') ~ '^\s*-?\d+\.\d+$' THEN
                        CAST(
                            REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9,.€$£\-]', ''), ',', '') AS DOUBLE PRECISION
                        )
                    -- Integer: e.g. "1234" or "-5678"
                    WHEN REGEXP_REPLACE(auftragswert, '[^0-9,.€$£]', '', 'g') ~ '^\s*-?\d+$' THEN
                        CAST(
                            REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9,.€$£\-]', ''), ',', '') AS DOUBLE PRECISION
                        )
                    ELSE NULL
                END
        END AS amount_raw

    FROM stage1
),
final AS (
    SELECT
        opp_kennung AS "Id",

        COALESCE(INITCAP(TRIM(titel)), 'Unnamed Opportunity') AS "Name",

        CASE phase_upper
            WHEN 'PROSPECTING' THEN 'Prospecting'
            WHEN 'PROSPECT'    THEN 'Prospecting'
            WHEN 'QUALIFICATION' THEN 'Qualification'
            WHEN 'QUALI'         THEN 'Qualification'
            WHEN 'QUALIFIKATION' THEN 'Qualification'
            WHEN 'IN PRÜFUNG'    THEN 'Needs Analysis'
            WHEN 'IN KONTAKT'    THEN 'Needs Analysis'
            WHEN 'WERTVONTRAG'   THEN 'Value Proposition'
            WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN 'MEHRWERTPROPOSITION' THEN 'Value Proposition'
            WHEN 'ID. ENTSCHEIDER' THEN 'Id. Decision Makers'
            WHEN 'IDENTIFY DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'ENTSCHEIDER'   THEN 'Id. Decision Makers'
            WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
            WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            WHEN 'ANGEBOT/PREISANGABE' THEN 'Proposal/Price Quote'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'BIETUNG'         THEN 'Proposal/Price Quote'
            WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
            WHEN 'NEGOTIATION/REVIEW'  THEN 'Negotiation/Review'
            WHEN 'GEWONNEN'   THEN 'Closed Won'
            WHEN 'WON'        THEN 'Closed Won'
            WHEN 'CLOSED WON' THEN 'Closed Won'
            WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
            WHEN 'VERLOREN'   THEN 'Closed Lost'
            WHEN 'LOST'       THEN 'Closed Lost'
            WHEN 'CLOSED LOST' THEN 'Closed Lost'
            WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
            ELSE 'Prospecting'
        END AS "StageName",

        COALESCE(close_date_raw, '') AS "CloseDate",

        amount_raw AS "Amount",

        CASE UPPER(TRIM(COALESCE(waehrungscode, '')))
            WHEN 'EUR'   THEN 'EUR'
            WHEN '€'     THEN 'EUR'
            WHEN 'USD'   THEN 'USD'
            WHEN 'DOLLAR' THEN 'USD'
            WHEN '$'     THEN 'USD'
            WHEN 'GBP'   THEN 'GBP'
            WHEN '£'     THEN 'GBP'
            WHEN 'CHF'   THEN 'CHF'
            WHEN 'SFR'   THEN 'CHF'
            ELSE NULL
        END AS "CurrencyIsoCode",

        kunden_ref AS "AccountId",

        opp_kennung AS "Legacy_Opportunity_ID__c",

        CURRENT_DATE::TEXT AS "CreatedDate",

        CURRENT_DATE::TEXT AS "LastModifiedDate",

        0 AS "IsDeleted"

    FROM stage2
)

SELECT * FROM final;