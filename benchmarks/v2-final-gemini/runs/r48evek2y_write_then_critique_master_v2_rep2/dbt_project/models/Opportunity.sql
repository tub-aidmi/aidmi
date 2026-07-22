{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        mo.opp_kennung,
        mo.titel,
        mo.vertriebsphase,
        mo.zieldatum,
        -- Clean and format auftragswert for Amount, handling European and US formats
        CASE
            -- Case 1: European format (e.g., 1.234,56) - has a comma as decimal separator, optionally dots for thousands
            WHEN TRIM(mo.auftragswert) ~ '^\s*-?\d{1,3}(\.\d{3})*,\d+$\s*' THEN
                REPLACE(REPLACE(TRIM(mo.auftragswert), '.', ''), ',', '.')
            -- Case 2: US/Standard format (e.g., 1,234.56) - has a dot as decimal separator, optionally commas for thousands
            WHEN TRIM(mo.auftragswert) ~ '^\s*-?\d{1,3}(,\d{3})*\.\d+$\s*' THEN
                REPLACE(TRIM(mo.auftragswert), ',', '')
            -- Case 3: Simple integer or decimal with dot (no thousands separators)
            WHEN TRIM(mo.auftragswert) ~ '^\s*-?\d+(\.\d+)?$\s*' THEN
                TRIM(mo.auftragswert)
            -- Fallback: Remove all non-numeric, non-dot, non-comma characters and try to clean
            ELSE
                REPLACE(REPLACE(REGEXP_REPLACE(TRIM(mo.auftragswert), '[^0-9,.]', '', 'g'), '.', ''), ',', '.')
        END AS cleaned_auftragswert,
        mo.waehrungscode,
        mo.kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
)
SELECT
    co.opp_kennung AS "Id",
    COALESCE(co.titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(co.vertriebsphase) = 'qualifizierung' THEN 'Qualification'
        WHEN LOWER(co.vertriebsphase) = 'angebot' THEN 'Proposal/Price Quote'
        WHEN LOWER(co.vertriebsphase) = 'verhandlung' THEN 'Negotiation/Review'
        WHEN LOWER(co.vertriebsphase) = 'gewonnen' THEN 'Closed Won'
        WHEN LOWER(co.vertriebsphase) = 'verloren' THEN 'Closed Lost'
        -- Fallback for NULL or unmapped values to satisfy NOT NULL constraint
        ELSE 'Prospecting'
    END AS "StageName",
    -- Parse zieldatum with multiple formats and fall back to current date if unparseable or NULL
    COALESCE(
        TO_CHAR(TO_DATE(NULLIF(TRIM(co.zieldatum), ''), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(NULLIF(TRIM(co.zieldatum), ''), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(NULLIF(TRIM(co.zieldatum), ''), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL target
    ) AS "CloseDate",
    -- Cast cleaned amount to DOUBLE PRECISION if it matches a valid numeric pattern
    CASE
        WHEN co.cleaned_auftragswert ~ '^-?\d+(\.\d+)?$' THEN CAST(co.cleaned_auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    COALESCE(co.waehrungscode, 'USD') AS "CurrencyIsoCode", -- Default to USD if not provided
    co.kunden_ref AS "AccountId", -- Maps to kundennummer from master_kunden, which serves as Salesforce Account Id
    co.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No source column
    NULL AS "LastModifiedDate", -- No source column
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS co
```