{{ config(materialized='table') }}

SELECT
    MD5(TRIM(o.opp_kennung)) AS "Id",
    COALESCE(TRIM(o.titel), 'Opportunity ' || TRIM(o.opp_kennung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('in kontakt', 'qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('angebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('abgeschlossen (gewonnen)', 'closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('abgeschlossen (verloren)', 'lost', 'verloren') THEN 'Closed Lost'
        ELSE