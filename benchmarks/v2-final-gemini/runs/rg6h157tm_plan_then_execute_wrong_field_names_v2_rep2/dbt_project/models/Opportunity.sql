WITH source_chancen AS (
    SELECT
        ap_id,
        chance_id,
        bezeichnung,
        phase,
        abschlussdatum,
        volumen,
        waehrung,
        kd_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
    MD5(chancen.chance_id) AS "Id",
    COALESCE(TRIM(chancen.bezeichnung), 'Opportunity - ' || chancen.chance_id) AS "Name",
    CASE LOWER(TRIM(chancen.phase))
        WHEN 'interessent' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wertangebot' THEN 'Value Proposition'
        WHEN 'entscheider_identifiziert' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    COALESCE(TRIM(UPPER(chancen.waehrung)), 'USD') AS "CurrencyIsoCode",
    MD5(chancen.kd_nr) AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_chancen AS chancen