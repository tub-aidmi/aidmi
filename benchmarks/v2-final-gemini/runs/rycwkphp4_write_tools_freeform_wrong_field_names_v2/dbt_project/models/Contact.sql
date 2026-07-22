-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName", -- LastName is NOT NULL
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE funktion
        WHEN 'Decision Maker' THEN 'Decision Maker'
        WHEN 'End User' THEN 'End User'
        WHEN 'Technical Contact' THEN 'Technical Contact'
        WHEN 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE sprache
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'DE' THEN 'DE'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId", -- Maps to kunden.kunden_nr
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
