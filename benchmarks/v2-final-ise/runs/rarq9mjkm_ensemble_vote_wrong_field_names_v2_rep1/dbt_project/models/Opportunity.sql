{{ config(materialized='table') }}

SELECT
    "chance_id" AS "Id",
    "bezeichnung" AS "Name",
    CASE
        WHEN LOWER(TRIM("phase")) = 'prospektierung' THEN 'Prospecting'
        WHEN LOWER(TRIM("phase")) = 'qualifizierung' THEN 'Qualification'
        WHEN LOWER(TRIM("phase")) = 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN LOWER(TRIM("phase")) = 'wertangebot' THEN 'Value Proposition'
        WHEN LOWER(TRIM("phase")) = 'entscheider identifizieren' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM("phase")) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN LOWER(TRIM("phase")) = 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM("phase")) = 'verhandlung/prüfung' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM("phase")) = 'abgeschlossen gewonnen' THEN 'Closed Won'
        WHEN LOWER(TRIM("phase")) = 'abgeschlossen verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN "abschlussdatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN "abschlussdatum"
        WHEN "abschlussdatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("abschlussdatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "abschlussdatum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE("abschlussdatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    "volumen" AS "Amount",
    "waehrung" AS "CurrencyIsoCode",
    "kunden"."kunden_nr" AS "AccountId",
    "chance_id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS "chancen"
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS "kunden"
    ON "chancen"."kd_nr" = "kunden"."kunden_nr"