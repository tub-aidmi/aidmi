{{ config(materialized='table') }}

WITH opp_src AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
acct_src AS (
    SELECT
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
    WHERE kundennummer IS NOT NULL
      AND TRIM(kundennummer) <> ''
)

SELECT
    -- Id: Salesforce-style 15-digit Id with prefix '006'
    CAST('006' || LPAD(TRIM(opp.opp_kennung), 9, '0') AS TEXT) AS "Id",
    -- Name: title, capitalized
    INITCAP(TRIM(opp.titel)) AS "Name",
    -- StageName: map German sales phases to standard CRM stages
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%neu%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%lead%' THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%qualif%' THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%bedarf%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%analyse%'  THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%wert%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%konzept%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%vorschlag%' THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%entscheider%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%verantwortl%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%decision%'  THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%perception%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%bewertung%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%analyse (soll)%' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%angebot%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%offerte%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%preis%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%proposal%'  THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%verhandlung%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%review%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%korrektur%'  THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%gewon%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%auftrag%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%closed won%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%erfolgreich%' THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) LIKE '%verloren%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%ablehn%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%closed lost%'
          OR LOWER(TRIM(opp.vertriebsphase)) LIKE '%abgebrochen%' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    -- CloseDate: parse DD.MM.YYYY or YYYYMMDD → ISO YYYY-MM-DD
    CASE
        WHEN TRIM(opp.zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$'
          AND LENGTH(TRIM(opp.zieldatum)) = 10
        THEN TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(opp.zieldatum) ~ '^\d{8}$'
          AND LENGTH(TRIM(opp.zieldatum)) = 8
        THEN TO_CHAR(TO_DATE(TRIM(opp.zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    -- Amount: strip all non-numeric chars except . and ,, detect European vs simple format, then cast
    CASE
        WHEN TRIM(opp.auftragswert) IS NOT NULL AND TRIM(opp.auftragswert) <> '' THEN
            -- First strip everything except digits, dots, commas
            CASE
                WHEN LENGTH(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d.,]', '', 'g')) = 0 THEN NULL
                ELSE
                    CASE
                        -- European format: contains a comma (decimal separator) possibly preceded by dot-separated thousands
                        -- e.g. "14.234,56" or "EUR 14.234,56" → remove non-digits first
                        WHEN REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d.,]', '', 'g') ~ '\.[0-9]{3},[0-9]' THEN
                            -- European: remove thousand-sep dots, then comma→period for casting
                            CAST(REGEXP_REPLACE(
                                REPLACE(
                                    REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d.,]', '', 'g'),
                                    '.', ''   -- strip all dots (thousands separators)
                                ),
                                ',', '.'   -- swap comma to decimal point
                            ) AS DOUBLE PRECISION)
                        ELSE
                            -- Simple format: strip everything except digits, cast as-is
                            CAST(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^\d]', '', 'g') AS DOUBLE PRECISION)
                    END
            END
        ELSE NULL
    END AS "Amount",
    -- CurrencyIsoCode: uppercase and trim; guard against empty strings / pure text codes that fail as ISO currency
    CASE
        WHEN TRIM(opp.waehrungscode) IS NOT NULL
          AND TRIM(opp.waehrungscode) <> '' THEN
            UPPER(TRIM(opp.waehrungscode))
        ELSE NULL
    END AS "CurrencyIsoCode",
    -- AccountId: Salesforce-style 15-digit Id from customer number, padded with '001' prefix
    CAST('001' || LPAD(TRIM(cust.kundennummer), 15, '0') AS TEXT) AS "AccountId",
    -- Legacy_Opportunity_ID__c: raw natural key for row-level validation
    TRIM(opp.opp_kennung) AS "Legacy_Opportunity_ID__c",
    -- Timestamps and soft-delete flag
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opp_src opp
LEFT JOIN acct_src cust
    ON TRIM(opp.kunden_ref) = TRIM(cust.kundennummer)