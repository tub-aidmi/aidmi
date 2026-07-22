-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}

{{ config(materialized='table') }}

SELECT
    ansprechpartner.ap_id AS "Id",
    ansprechpartner.vorname AS "FirstName",
    COALESCE(ansprechpartner.nachname, 'Unknown') AS "LastName",
    ansprechpartner.email_adresse AS "Email",
    ansprechpartner.telefonnummer AS "Phone",
    ansprechpartner.position AS "Title",
    CASE
        WHEN ansprechpartner.funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN ansprechpartner.funktion
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(ansprechpartner.sprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(ansprechpartner.sprache)
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(ansprechpartner.kunde) AS "AccountId",
    ansprechpartner.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ansprechpartner