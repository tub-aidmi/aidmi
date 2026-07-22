{{ config(materialized='table') }}
SELECT
    MD5(opp.opp_kennung) AS "Id",
    INITCAP(TRIM(opp.titel)) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('prospecting', 'in kontakt', 'in kontakt ', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('id. decision makers', 'entscheidungsträger identifizieren') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('proposal/price quote', 'angebot/preisangebot', 'in prüfung') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed won', 'abgeschlossen (gewonnen)', 'won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed lost', 'abgeschlossen (verloren)', 'lost', 'verloren') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(opp.auftragswert) = 'None' OR TRIM(opp.auftragswert) = '' OR TRIM(opp.auftragswert) IS NULL THEN NULL
        ELSE
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(opp.auftragswert, '[^0-9.,-]', '', 'g'),
                                '^\D*',
                                '',
                                'g'
                            ),
                            '\.',
                            '',
                            'g'
                        ),
                        ',',
                        '.',
                        'g'
                    )
                AS DOUBLE PRECISION
            )
    END AS "Amount",
    CASE
        WHEN TRIM(UPPER(opp.waehrungscode)) IN ('EUR', '€', 'EURO') THEN 'EUR'
        WHEN TRIM(UPPER(opp.waehrungscode)) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN TRIM(UPPER(opp.waehrungscode)) IN ('GBP', '£') THEN 'GBP'
        WHEN TRIM(UPPER(opp.waehrungscode)) IN ('CHF') THEN 'CHF'
        ELSE TRIM(UPPER(opp.waehrungscode))
    END AS "CurrencyIsoCode",
    COALESCE(MD5(kund.kundennummer), MD5('UNKNOWN')) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} kund
    ON REGEXP_REPLACE(opp.kunden_ref, '^KD-M?', '') = REGEXP_REPLACE(kund.kundennummer, '^CUST-M?', '')