-- This dbt model transforms the ansprechpartner source table into the Contact target schema.

{{ config(materialized='table') }}

SELECT
    ansprechpartner.ap_id AS "Id",
    ansprechpartner.vorname AS "FirstName",
    COALESCE(ansprechpartner.nachname, 'Unknown') AS "LastName",
    ansprechpartner.email_adresse AS "Email",
    ansprechpartner.telefonnummer AS "Phone",
    ansprechpartner.position AS "Title",
    CASE
        WHEN LOWER(TRIM(ansprechpartner.funktion)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ansprechpartner.funktion)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(ansprechpartner.funktion)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ansprechpartner.funktion)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ansprechpartner.sprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(ansprechpartner.sprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(ansprechpartner.sprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(ansprechpartner.sprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(ansprechpartner.sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ansprechpartner.kunde AS "AccountId",
    ansprechpartner.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ansprechpartner