{{ config(materialized='table') }} SELECT
    trim(o.chance_id) AS "Id",
    coalesce(trim(o.bezeichnung), 'Unknown Opportunity ' || trim(o.chance_id)) AS "Name",
    CASE
        WHEN trim(o.phase) = 'Prospecting' THEN 'Prospecting'
        WHEN trim(o.phase) = 'Qualification' THEN 'Qualification'
        WHEN trim(o.phase) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN trim(o.phase) = 'Value Proposition' THEN 'Value Proposition'
        WHEN trim(o.phase) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN trim(o.phase) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN trim(o.phase) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN trim(o.phase) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN trim(o.phase) = 'Closed Won' THEN 'Closed Won'
        WHEN trim(o.phase) = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    coalesce(
        to_char(
            CASE
                WHEN o.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(o.abschlussdatum, 'YYYY-MM-DD')
                WHEN o.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(o.abschlussdatum, 'DD.MM.YYYY')
                WHEN o.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(o.abschlussdatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        '1900-01-01' -- Default date for NOT NULL constraint if source date is unparseable
    ) AS "CloseDate",
    o.volumen AS "Amount",
    trim(o.waehrung) AS "CurrencyIsoCode",
    MD5(k.kunden_nr || k.firmenname) AS "AccountId",
    trim(o.chance_id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS o
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    o.kd_nr = k.kunden_nr