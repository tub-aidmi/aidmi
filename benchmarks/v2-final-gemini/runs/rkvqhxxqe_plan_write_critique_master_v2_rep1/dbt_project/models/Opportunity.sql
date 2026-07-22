{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        opps.opp_kennung,
        opps.titel,
        opps.vertriebsphase,
        opps.zieldatum,
        opps.waehrungscode,
        -- Pre-clean auftragswert to simplify subsequent parsing
        TRIM(REGEXP_REPLACE(opps.auftragswert, '[^0-9.,]+', '', 'g')) AS cleaned_auftragswert,
        kunden.kundennummer
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opps
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
        ON opps.kunden_ref = kunden.kundennummer
)
SELECT
    MD5(opps.opp_kennung) AS "Id",
    COALESCE(TRIM(opps.titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('id. decision makers', 'identifizierung von entscheidungsträgern') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('proposal/price quote', 'angebot/preiskalkulation') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('closed won', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('closed lost', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE WHEN opps.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opps.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN opps.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opps.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN opps.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opps.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN opps.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opps.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') ELSE NULL END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CAST(
        NULLIF(
            CASE
                WHEN opps.cleaned_auftragswert IS NULL OR opps.cleaned_auftragswert = '' THEN NULL
                WHEN POSITION('.' IN opps.cleaned_auftragswert) > 0 AND POSITION(',' IN opps.cleaned_auftragswert) > 0 THEN
                    CASE
                        WHEN POSITION(',' IN opps.cleaned_auftragswert) > POSITION('.' IN opps.cleaned_auftragswert) THEN
                            -- European format (e.g., 1.234,56 -> 1234.56)
                            REPLACE(REPLACE(opps.cleaned_auftragswert, '.', ''), ',', '.')
                        ELSE
                            -- US format (e.g., 1,234.56 -> 1234.56)
                            REPLACE(opps.cleaned_auftragswert, ',', '')
                    END
                WHEN POSITION(',' IN opps.cleaned_auftragswert) > 0 THEN
                    -- Only comma present, assume European decimal (e.g., 1234,56 -> 1234.56)
                    REPLACE(opps.cleaned_auftragswert, ',', '.')
                ELSE
                    -- Only dot, or neither, assume US decimal (e.g., 1234.56 or 1234)
                    opps.cleaned_auftragswert
            END,
            ''
        ) AS DOUBLE PRECISION
    ) AS "Amount",
    TRIM(UPPER(opps.waehrungscode)) AS "CurrencyIsoCode",
    MD5(opps.kundennummer) AS "AccountId",
    opps.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS opps
