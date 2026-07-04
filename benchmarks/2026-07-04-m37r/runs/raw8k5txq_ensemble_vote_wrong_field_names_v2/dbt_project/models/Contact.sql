-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    ansp.ap_id AS "Id",
    ansp.vorname AS "FirstName",
    COALESCE(ansp.nachname, 'Unknown') AS "LastName",
    ansp.email_adresse AS "Email",
    ansp.telefonnummer AS "Phone",
    ansp.position AS "Title",
    CASE
        WHEN ansp.funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN ansp.funktion
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ansp.sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN ansp.sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    ansp.kunde AS "AccountId",
    ansp.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ansp