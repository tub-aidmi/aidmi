WITH cleaned_opportunities AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.vertriebsphase,
        o.zieldatum,
        o.waehrungscode,
        o.kunden_ref,
        REGEXP_REPLACE(o.auftragswert, '[^0-9.,]+', '', 'g') AS cleaned_auftragswert
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
)
SELECT
    MD5(co.opp_kennung) AS "Id",
    COALESCE(TRIM(co.titel), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(co.vertriebsphase) IN ('won', 'gewonnen', 'abgeschlossen (gewonnen)', 'closed won', 'closed_won') THEN 'Closed Won'
        WHEN LOWER(co.vertriebsphase) IN ('lost', 'verloren', 'abgeschlossen (verloren)', 'closed lost', 'closed_lost') THEN 'Closed Lost'
        WHEN LOWER(co.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(co.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(co.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE WHEN co.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD') END,
        CASE WHEN co.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') END,
        CASE WHEN co.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') END,
        CASE WHEN co.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for unparseable dates, as CloseDate is NOT NULL
    ) AS "CloseDate",
    CAST(
        NULLIF(
            CASE
                -- Case 1: European format with thousands dot and decimal comma (e.g., "1.234,56")
                -- Condition: contains both '.' and ',' AND '.' appears before ','
                WHEN co.cleaned_auftragswert ~ '\.' AND co.cleaned_auftragswert ~ ',' AND POSITION('.' IN co.cleaned_auftragswert) < POSITION(',' IN co.cleaned_auftragswert) THEN
                    REPLACE(REPLACE(co.cleaned_auftragswert, '.', ''), ',', '.')
                -- Case 2: Only comma, assume European decimal separator (e.g., "123,45")
                -- Condition: contains only ',' (no '.')
                WHEN co.cleaned_auftragswert ~ ',' AND NOT co.cleaned_auftragswert ~ '\.' THEN
                    REPLACE(co.cleaned_auftragswert, ',', '.')
                -- Case 3: US format (thousands comma, decimal dot, e.g., "1,234.56") OR simple dot decimal (e.g., "123.45") OR no separators
                -- Condition: all other cases
                ELSE
                    REPLACE(co.cleaned_auftragswert, ',', '') -- Remove commas (e.g., 1,234.56 -> 1234.56, 123.45 -> 123.45)
            END,
            ''
        ) AS DOUBLE PRECISION
    ) AS "Amount",
    UPPER(co.waehrungscode) AS "CurrencyIsoCode",
    MD5(co.kunden_ref) AS "AccountId",
    co.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS co