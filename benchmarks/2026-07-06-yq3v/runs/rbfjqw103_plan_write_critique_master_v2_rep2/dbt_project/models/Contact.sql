{{ config(materialized='table') }}

SELECT
    gen_random_uuid()::text AS "Id",
    TRIM(INITCAP(mk.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(mk.familienname)), TRIM(INITCAP(mk.rufname)), 'Unknown Contact') AS "LastName",
    TRIM(LOWER(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(INITCAP(mk.berufsbezeichnung)) AS "Title",
    CASE UPPER(TRIM(mk.rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(mk.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    NULL AS "AccountId",
    TRIM(mk.kontakt_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
