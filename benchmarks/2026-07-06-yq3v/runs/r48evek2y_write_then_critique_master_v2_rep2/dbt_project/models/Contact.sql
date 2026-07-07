SELECT
    mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    COALESCE(mk.familienname, 'Unknown') AS "LastName",
    mk.kontakt_email AS "Email",
    mk.tel AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(mk.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(mk.rolle) IN ('end user', 'anwender', 'nutzer') THEN 'End User'
        WHEN LOWER(mk.rolle) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(mk.rolle) IN ('executive sponsor', 'vorstand', 'geschäftsführer') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(mk.korrespondenzsprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(mk.korrespondenzsprache)
        ELSE NULL
    END AS "Preferred_Language__c",
    mc.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mc
ON
    mk.kd_nummer = mc.kundennummer