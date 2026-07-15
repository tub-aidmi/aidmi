{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        ap.ap_id,
        TRIM(ap.vorname) AS vorname,
        TRIM(ap.nachname) AS nachname,
        TRIM(LOWER(ap.email_adresse)) AS email,
        REGEXP_REPLACE(ap.telefonnummer, '[^0-9+]', '', 'g') AS phone,
        TRIM(ap.position) AS position,
        TRIM(ap.funktion) AS funktion,
        UPPER(TRIM(ap.sprache)) AS sprache,
        ap.kunde
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
),
account_mapping AS (
    SELECT
        k.kunden_nr AS account_id,
        k.kunden_nr AS legacy_customer_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
)

SELECT
    MD5(ap_id || COALESCE(kunde, '')) AS "Id",
    INITCAP(COALESCE(vorname, '')) AS "FirstName",
    INITCAP(COALESCE(nachname, 'Unknown')) AS "LastName",
    email AS "Email",
    phone AS "Phone",
    INITCAP(COALESCE(position, '')) AS "Title",
    CASE
        WHEN funktion = 'Decision Maker' THEN 'Decision Maker'
        WHEN funktion = 'End User' THEN 'End User'
        WHEN funktion = 'Technical Contact' THEN 'Technical Contact'
        WHEN funktion = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    am.account_id AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_source cs
LEFT JOIN account_mapping am ON cs.kunde = am.legacy_customer_id