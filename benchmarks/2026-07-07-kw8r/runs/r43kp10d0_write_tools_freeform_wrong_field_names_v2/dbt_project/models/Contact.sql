{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT
        ap.ap_id,
        ap.vorname,
        ap.nachname,
        ap.email_adresse,
        ap.telefonnummer,
        ap.position,
        ap.funktion,
        ap.sprache,
        ap.kunde
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || LPAD(
            REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'),
            15,
            '0'
        ) AS account_sf_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    '003' || LPAD(
        REGEXP_REPLACE(c.ap_id, '[^0-9]', '', 'g'),
        15,
        '0'
    ) AS "Id",
    c.vorname AS "FirstName",
    c.nachname AS "LastName",
    c.email_adresse AS "Email",
    c.telefonnummer AS "Phone",
    c.position AS "Title",
    
    CASE 
        WHEN UPPER(c.funktion) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN UPPER(c.funktion) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(c.funktion) = 'END USER' THEN 'End User'
        WHEN UPPER(c.funktion) = 'DECISION MAKER' THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",
    
    CASE 
        WHEN UPPER(c.sprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') 
        THEN UPPER(c.sprache)
        ELSE NULL
    END AS "Preferred_Language__c",
    
    a.account_sf_id AS "AccountId",
    c.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM contact_data c
LEFT JOIN account_ids a ON c.kunde = a.kunden_nr
