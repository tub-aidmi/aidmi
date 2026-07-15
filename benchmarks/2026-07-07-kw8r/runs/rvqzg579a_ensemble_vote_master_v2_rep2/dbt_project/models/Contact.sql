{{ config(materialized='table') }}

SELECT
    gen_random_uuid() AS "Id",
    TRIM(INITCAP(mk."rufname")) AS "FirstName",
    TRIM(INITCAP(mk."familienname")) AS "LastName",
    TRIM(LOWER(mk."kontakt_email")) AS "Email",
    TRIM(mk."tel") AS "Phone",
    TRIM(INITCAP(mk."berufsbezeichnung")) AS "Title",
    CASE 
        WHEN TRIM(LOWER(mk."rolle")) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(mk."rolle")) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN TRIM(LOWER(mk."rolle")) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(mk."rolle")) IN ('executive sponsor', 'geschäftsführung') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(mk."korrespondenzsprache")) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN TRIM(LOWER(mk."korrespondenzsprache")) IN ('englisch', 'en', 'english') THEN 'EN'
        WHEN TRIM(LOWER(mk."korrespondenzsprache")) IN ('französisch', 'fr', 'french') THEN 'FR'
        WHEN TRIM(LOWER(mk."korrespondenzsprache")) IN ('spanisch', 'es', 'spanish') THEN 'ES'
        WHEN TRIM(LOWER(mk."korrespondenzsprache")) IN ('italienisch', 'it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    gen_random_uuid() AS "AccountId",
    mk."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mkd ON TRIM(mk."kd_nummer") = TRIM(mkd."kundennummer")