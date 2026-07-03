{{ config(materialized='table') }}

SELECT
    '001' || REGEXP_REPLACE(opp_kennung, '^OPP-M-', '') AS "Id",
    
    COALESCE(INITCAP(TRIM(titel)), 'Unknown Opportunity') AS "Name",
    
    COALESCE(
        CASE 
            WHEN LOWER(TRIM(vertriebsphase)) IN ('gewonnen', 'closed won', 'won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('verloren', 'lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
            WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    
    COALESCE(
        CASE 
            WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN NULL
            WHEN UPPER(TRIM(zieldatum)) IN ('N/A', 'NULL') THEN NULL
            WHEN TRIM(zieldatum) ~ '^0000-00-00$' THEN NULL
            WHEN zieldatum ~ '^\d{8}$' THEN TO_DATE(TRIM(zieldatum), 'YYYYMMDD')::TEXT
            WHEN zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')::TEXT
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
            ELSE NULL
        END,
        '2099-12-31'
    ) AS "CloseDate",
    
    CASE 
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        WHEN UPPER(TRIM(auftragswert)) IN ('NULL', 'NONE', 'N/A') THEN NULL
        ELSE
            CAST(
                CASE
                    WHEN TRIM(auftragswert) ~ ',' THEN
                        REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    TRIM(auftragswert),
                                    '^(EUR|USD|CHF|GBP|€|\$|Dollar)\s*',
                                    '', 'i'
                                ),
                                '\.',
                                '',
                                'g'
                            ),
                            ',',
                            '.'
                        )
                    ELSE
                        REGEXP_REPLACE(
                            TRIM(auftragswert),
                            '^(EUR|USD|CHF|GBP|€|\$|Dollar)\s*',
                            '', 'i'
                        )
                END
                AS DOUBLE PRECISION
            )
    END AS "Amount",
    
    CASE 
        WHEN LOWER(TRIM(waehrungscode)) IN ('eur', '€') THEN 'EUR'
        WHEN LOWER(TRIM(waehrungscode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(waehrungscode)) IN ('chf', 'fr.') THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    
    CASE 
        WHEN kunden_ref ~ '^KD-M' THEN
            '001' || REGEXP_REPLACE(kunden_ref, '^KD-M', '')
        ELSE NULL
    END AS "AccountId",
    
    opp_kennung AS "Legacy_Opportunity_ID__c",
    
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_opportunities') }}