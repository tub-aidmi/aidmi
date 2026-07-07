-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(o.stagename) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN TRIM(o.stagename)
        ELSE 'Prospecting' -- Default to a valid enum value if source is invalid or NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(o.closedate AS DATE), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default to a valid date if source is unparseable or NULL, as target is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN REPLACE(REPLACE(o.amount, ',', ''), '.', '') ~ '^\d+$' THEN CAST(REPLACE(REPLACE(o.amount, ',', ''), '.', '') AS DOUBLE PRECISION)
        WHEN REPLACE(o.amount, '.', '') ~ '^\d+,\d{2}$' THEN CAST(REPLACE(REPLACE(o.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o