# OWL Match Stat Log

Make FinalStat tables and Ultimate PETHs for each match and save them into the DB

## How to Use
1. Execute OWL Event Stream Data Parser (https://github.com/yongcheoljeong/owl-esd-to-db)

2. Set 

```python
# FinalStat
match_sql = MatchLog().update_FinalStat_to_sql(if_exists='pass') # if you want to replace the previous tables, use if_exists='replace'
```

3. Run `main.ipynb`

---

# Documentations

## Stat Level
1. WorkshopStat (level 0, from `esd_phs`)
- 'TimePlayed':33,
- 'HeroDamageDealt':1207,
- 'BarrierDamageDealt':1301,
- 'HeroDamageTaken':401,
- 'Deaths':42,
- 'Eliminations':31,
- 'FinalBlows':43,
- 'EnvironmentalDeaths':869,
- 'EnvironmentalKills':866,
- 'HealingDealt':449,
- 'ObjectiveKills':796,
- 'SoloKills':45,
- 'UltimatesEarned':1122,
- 'UltimatesUsed':1123,
- 'HealingReceived':1716,
- 'DefensiveAssists':986,
- 'OffensiveAssists':980

2. TraditionalStat (level 1)
- AllDamageDealt: `HeroDamageDealt`+`BarrierDamageDealt`
- HealingReceived: this is required to be preprocessed because all WorkshopStats but HealingReceived is aggregated by `Hero` level, not `Player` level.
- HealthPercent
- NumAlive (number of living players of a team)

3. AdvancedStat (level 2)
- RCPv1 (Relative Combat Power version1): `RCP = (X**2 - Y**2).div(Max)` where X (NumAlive of X team) and Y (NumAlive of Y Team)
- FBValue (Final Blow Value): `FB_value = abs(RCP(X, Y + FB) - RCP(X, Y))`
- DeathRisk: `Death_risk = abs(RCP(X + Death, Y) - RCP(X, Y))`
- DIv2 (Dominance Index version2): `DI = X.mean()['TF_RCP_sum/s']`

## Teamfight Detector
1. TFWinnerDetector
- `TF_RCP_weighted_sum`: weigh the RCP of a teamfight to evaluate the teamfight winner
- `TF_RCP_weighted_sum > 0`: teamfight winner
- `TF_RCP_weighted_sum < 0`: teamfight loser
- `TF_RCP_weighted_sum == 0`: draw

2. TF_Detector
- `start_condition`
    - cond0: TF is not ongoing
    - cond1: time > previous TF end_time
    - cond2: HDD > HDD_threshold
    - cond3: FB > 0
    - cond4: HDD in increasing trend
    - cond5: HDD is min within no_FB_duration

    `start_condition = condition0() & condition1(idx) & ( (condition2(idx) & condition4(idx) & condition5(idx)) | condition3(idx) ) `

- `end_condition`
    - cond0: TF is ongoing
    - cond1: FB == 0 for over no_FB_duration
    - cond2: time == map_end_time
    - cond3: FB > 0
    - cond4: while FB == 0 for 0~+8 & HDD <= {HDD_lull_cut}
    - cond5: mean(HDD) < HDD_threshold for 2 sec

    `end_condition = ( condition0() & ((condition1(idx) & condition5(idx)) & FB_happened) | (FB_happened & condition4(idx)) ) | condition2(idx)`
