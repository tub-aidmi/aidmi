SELECT
    ansprechpartner.ap_id AS "Id",
    ansprechpartner.vorname AS "FirstName",
    COALESCE(ansprechpartner.nachname, 'Unknown') AS "LastName",
    ansprechpartner.email_adresse AS "Email",
    ansprechpartner.telefonnummer AS "Phone",
    ansprechpartner.position AS "Title",
    CASE
        WHEN LOWER(ansprechpartner.funktion) LIKE '%decision%' THEN 'Decision Maker'
        WHEN LOWER(ansprechpartner.funktion) LIKE '%end user%' THEN 'End User'
        WHEN LOWER(ansprechpartner.funktion) LIKE '%technical%' THEN 'Technical Contact'
        WHEN LOWER(ansprechpartner.funktion) LIKE '%executive%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(ansprechpartner.sprache) = 'de' THEN 'DE'
        WHEN LOWER(ansprechpartner.sprache) = 'en' THEN 'EN'
        WHEN LOWER(ansprechpartner.sprache) = 'fr' THEN 'FR'
        WHEN LOWER(ansprechpartner.sprache) = 'es' THEN 'ES'
        WHEN LOWER(ansprechpartner.sprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunden.kunden_nr AS "AccountId",
    ansprechpartner.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    وَsource('fixture_wrong_field_names_v2_src', 'ansprechpartner')ğ AS ansprechpartner
LEFT JOIN
    وَsource('fixture_wrong_field_names_v2_src', 'kunden')ğ AS kunden
    ON ansprechpartner.kunde = kunden.kunden_nr