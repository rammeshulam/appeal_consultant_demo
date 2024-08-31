CREATE OR REPLACE TABLE `appeal_consultant_demo.verdicts_clean` AS (
SELECT
  CaseId,
  text,
  summary,
  VerdictsDt,
  #JSON_QUERY(summary, '$.response.crime_categories') AS `קטגוריות ההאשמות`,
  JSON_QUERY(summary, '$.response.crime_categories[0]') AS `CrimeCateogry1`, #`קטגורית ההאשמות 1`,
  JSON_QUERY(summary, '$.response.crime_categories[1]') AS `CrimeCateogry2`, #`קטגורית ההאשמות 2`,
  JSON_QUERY(summary, '$.response.crime_categories[2]') AS `CrimeCateogry3`, #`קטגורית ההאשמות 3`,
  JSON_QUERY(summary, '$.response.summary') AS `CaseSummary`, #`סיכום התיק`,
  JSON_QUERY(summary, '$.response.reason_for_appeal') AS `ReasonForAppeal`, # `סיכום הסיבה לערעור`,
  CAST(JSON_QUERY(summary, '$.response.is_extenuating_circumstances') AS BOOL) AS `IsExtenuatingCircumstances`, #`טענה לנסיבות מקלות`,
  CAST(JSON_QUERY(summary, '$.response.is_new_evidence') AS BOOL) AS `IsNewEvidence`, # `טענה לראיות חדשות`,
  CAST(JSON_QUERY(summary, '$.response.is_personal_circumstances') AS BOOL) AS `IsPersonalCircumstances`, #`טענה לנסיבות אישיות מקלות`,
  CAST(JSON_QUERY(summary, '$.response.is_punishment_does_not_match_crime') AS BOOL) AS `IsPunishmentDoesNotMatchCrime`, # `טענה שהעונש לא תואם את הפשע`,
  JSON_QUERY(summary, '$.response.sections_of_Penal_Code') AS `SectionsOfPenalCode`, #`סעיפי עונשין המוזכרים בערעור`,
  JSON_QUERY(summary, '$.response.punishment') AS `Punishment`, #`גזר דין`
FROM `appeal_consultant_demo.verdicts` 
)