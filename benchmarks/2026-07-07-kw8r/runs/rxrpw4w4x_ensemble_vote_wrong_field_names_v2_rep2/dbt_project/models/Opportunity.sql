{{ config(materialized='table') }}

SELECT 
    c.chance_id AS "Id",
    COALESCE(UPPER(TRIM(c.bezeichnung)), 'Unknown Opportunity') AS "Name",
    CASE 
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('prospecting', 'prospektierung', 'lead generation', 'lead') THEN 'Prospecting'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('qualification', 'qualifikation', 'qualifying', 'verifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('needs analysis', 'bedarfsanalyse', 'requirement analysis', 'bedarfsermittlung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('value proposition', 'wertvorschlag', 'value proposition creation') THEN 'Value Proposition'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('id. decision makers', 'decision makers identified', 'entscheider identifiziert', 'identifikation entscheidungsträger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('perception analysis', 'wahrnehmungsanalyse', 'analysis', 'bewertung') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('proposal/price quote', 'angebot/preisangebot', 'offer', 'preisanfrage') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('negotiation/review', 'verhandlung/prüfung', 'negotiation', 'prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('closed won', 'gewonnen', 'won', 'erfolgreich abgeschlossen', 'abgeschlossen') THEN 'Closed Won'
        WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('closed lost', 'verloren', 'lost', 'gescheitert', 'nicht erfolgreich') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(COALESCE(c.waehrung, ''))) AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)