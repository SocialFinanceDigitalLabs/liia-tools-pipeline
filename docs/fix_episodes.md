|Stage|Rule|Action|Note
|---|---|---|---
|Stage1|RULE_1|Set DEC = DECOM_next <br> Set REC = 'X1' <br> If RNE_next in list (P,B,T,U) then <br> Set REASON_PLACE_CHANGE = 'LIIAF' (LIIA fix)| Child remains LAC but episode changes
|Stage1|RULE_1A|"Set DEC = min(31/03/YEAR, DECOM_next - 1day) <br> Set REC = 'E99' (LIIA fix)"	Child ceases LAC but re-enters care later| Child ceases LAC but re-enters care later
|Stage1|RULE_2|"Set DEC = 31/03/YEAR <br> Set REC = 'E99' (LIIA fix)"	Child no longer LAC|Child no longer LAC
|Stage1|RULE_3|Delete episode|Duplicate episode
|Stage1|RULE_3A|Delete episode|Episode replaced in later submission
|Stage2|RULE_4|Set DEC = DECOM_next|Overlaps next episode
|Stage2|RULE_5|Set DEC = DECOM_next|Gap between episodes which should be continuous (X1)