# Bewertung: Zeitreihen-Analysen im Plugin vs. Delegation an Prometheus

## Ueberblick

Das Plugin fuehrt aktuell **23 verschiedene Zeitreihen-Analysen** durch, verteilt auf 7 Kategorien.
Dieses Dokument bewertet jede Analyse hinsichtlich:

- **PromQL-Aequivalent:** Existiert eine native Prometheus-Funktion?
- **Performance-Gewinn:** Wie gross waere der Vorteil bei Delegation?
- **Erweiterbarkeitsrisiko:** Was geht verloren, wenn die Logik in Prometheus wandert?
- **Empfehlung:** Plugin behalten / An Prometheus delegieren / Hybrid

### Bewertungsskala

| Symbol | Bedeutung |
|--------|-----------|
| ++ | Sehr hoher Performance-Gewinn bei Delegation |
| + | Moderater Performance-Gewinn |
| o | Kein wesentlicher Unterschied |
| - | Erweiterbarkeit stark eingeschraenkt |
| -- | Delegation nicht moeglich/sinnvoll |

---

## 1. Einfache Aggregationen (Post-Processing auf bereits geholten Daten)

Diese Operationen werden ueber `AggregationUtil.apply()` als **Post-Processing** auf bereits
vom Plugin geholte Zeitreihen angewandt. Sie arbeiten auf dem `TimeSeriesResult`, das aus
Prometheus (oder einer anderen Quelle) bereits vorliegt.

### 1.1 Moving Average

| | |
|---|---|
| **Datei** | `aggregation_functions/MovingAverage.java` |
| **Procedure** | `graphobs.aggregation.moving_average` |
| **Algorithmus** | Gleitender Mittelwert ueber Fenster fester Groesse (Anzahl Punkte) |
| **PromQL** | `avg_over_time(metric[window])` — aber zeitbasiert, nicht punktbasiert |
| **Performance** | o — Daten liegen bereits im Plugin vor, Berechnung ist O(n) |
| **Erweiterbarkeit** | Die Plugin-Implementierung arbeitet punktbasiert (z.B. "5 Punkte"), PromQL arbeitet zeitbasiert (z.B. "5m"). Das ist semantisch verschieden. Andere TSDBs (InfluxDB, TimescaleDB) haben ebenfalls eigene Moving-Average-Funktionen mit leicht unterschiedlicher Semantik (z.B. `MOVING_AVERAGE()` in InfluxQL). |
| **Empfehlung** | **Plugin behalten.** Die Daten sind bereits geladen, der Rechenaufwand ist minimal. Die punktbasierte Semantik ist ein Feature, das PromQL nicht bietet. |

### 1.2 Binned Average

| | |
|---|---|
| **Datei** | `aggregation_functions/BinnedAverage.java` |
| **Procedure** | `graphobs.aggregation.binned_average` |
| **Algorithmus** | Aggregation in regelmaessige Zeitintervalle (Bins), Mittelwert pro Bin |
| **PromQL** | `avg_over_time(metric[interval])` — nahezu identisch |
| **Performance** | + — Prometheus koennte dies bei der Abfrage bereits berechnen und weniger Daten uebertragen |
| **Erweiterbarkeit** | InfluxDB: `GROUP BY time(interval)`. TimescaleDB: `time_bucket()`. Alle TSDBs haben native Bin-Aggregation. |
| **Empfehlung** | **Hybrid moeglich.** Fuer reine Prometheus-Quellen koennte der `step`-Parameter in `query_range` bereits als Binning fungieren. Fuer Daten aus anderen Quellen bleibt die Plugin-Implementierung noetig. Aktuell kein Handlungsbedarf, da die Berechnung Post-Processing ist. |

### 1.3 Cumulative Sum

| | |
|---|---|
| **Datei** | `aggregation_functions/CumulativeSum_Integral.java` |
| **Procedure** | `graphobs.aggregation.cumulative_sum` |
| **Algorithmus** | Laufende Summe: S[i] = S[i-1] + v[i] |
| **PromQL** | Kein direktes Aequivalent. `sum_over_time()` berechnet die Gesamtsumme, nicht die laufende. |
| **Performance** | o — O(n), Daten bereits vorliegend |
| **Erweiterbarkeit** | InfluxDB: `CUMULATIVE_SUM()`. Kein PromQL-Pendant. |
| **Empfehlung** | **Plugin behalten.** Prometheus kann das nicht. |

### 1.4 Integral (zeitgewichtete Summe)

| | |
|---|---|
| **Datei** | `aggregation_functions/CumulativeSum_Integral.java` |
| **Procedure** | `graphobs.aggregation.integral` |
| **Algorithmus** | Integral = Summe(Wert[i] * dt[i]) in Sekunden |
| **PromQL** | Kein Aequivalent. |
| **Performance** | o |
| **Erweiterbarkeit** | InfluxDB: `INTEGRAL()`. Prometheus: nicht vorhanden. |
| **Empfehlung** | **Plugin behalten.** |

### 1.5 Difference (Erste Differenz)

| | |
|---|---|
| **Datei** | `aggregation_functions/Difference_Derivative.java` |
| **Procedure** | `graphobs.aggregation.difference` |
| **Algorithmus** | d[i] = v[i] - v[i-1] |
| **PromQL** | `delta(metric[range])` — aehnlich, aber berechnet Differenz ueber ein Zeitfenster, nicht punktweise |
| **Performance** | o |
| **Erweiterbarkeit** | Alle TSDBs unterstuetzen aehnliche Operationen mit leicht unterschiedlicher Semantik. |
| **Empfehlung** | **Plugin behalten.** Punktweise Differenz != `delta()`. |

### 1.6 Derivative (Aenderungsrate pro Sekunde)

| | |
|---|---|
| **Datei** | `aggregation_functions/Difference_Derivative.java` |
| **Procedure** | `graphobs.aggregation.derivative` |
| **Algorithmus** | d[i] = (v[i] - v[i-1]) / (t[i] - t[i-1]) in Sekunden |
| **PromQL** | `rate(metric[range])` oder `deriv(metric[range])` — semantisch nah |
| **Performance** | + — `rate()` ist Prometheus' Kernkompetenz und sehr optimiert |
| **Erweiterbarkeit** | `rate()` existiert in jeder TSDB, aber mit unterschiedlicher Counter-Reset-Logik. Die Plugin-Variante ist naiv (keine Counter-Reset-Erkennung), was fuer Gauges korrekt ist. |
| **Empfehlung** | **Plugin behalten fuer Post-Processing.** Fuer Prometheus-Counter-Metriken wird `rate()` bereits beim Fetch in `buildSingleQueryExpression()` verwendet. Die Plugin-Derivative ist fuer Gauge-Daten gedacht und arbeitet auf bereits geholten Daten. |

### 1.7 Linear Regression (Trendlinie)

| | |
|---|---|
| **Datei** | `aggregation_functions/LinerRegression.java` |
| **Procedure** | `graphobs.aggregation.linear_regression` |
| **Algorithmus** | Least-Squares: y = slope*x + intercept, gibt gefittete Werte zurueck |
| **PromQL** | `predict_linear(metric[range], seconds)` — nur Vorhersage, keine gefitteten Werte |
| **Performance** | o |
| **Erweiterbarkeit** | Prometheus bietet nur `predict_linear()` (Einzelwert-Vorhersage). Die Plugin-Version gibt die komplette gefittete Zeitreihe zurueck. |
| **Empfehlung** | **Plugin behalten.** PromQL bietet keine aequivalente Funktionalitaet. |

---

## 2. Multi-Serien-Aggregation (Cross-Pod/Cross-Node)

### 2.1 Service-Aggregation (sum/avg ueber Pods)

| | |
|---|---|
| **Datei** | `topological_aggregation/ServiceAggregation.java` + `MultiSeriesAggregator.java` |
| **Procedure** | `graphobs.data.get_all_for_service` |
| **Algorithmus** | SUM fuer rps/cpu_total, AVERAGE fuer latency/error_rate — mit linearer Interpolation auf gemeinsame Zeitachse |
| **PromQL** | `sum(metric{service="X"})` / `avg(metric{service="X"})` — **nativ und hoch performant** |
| **Performance** | **++** — Prometheus aggregiert serverseitig, uebertraegt nur 1 Ergebnis-Serie statt N. Bei 50 Pods: 50x weniger Datentransfer. |
| **Erweiterbarkeit** | **Hoch.** Wenn Zeitreihen aus verschiedenen Quellen kommen (z.B. Pod A aus Prometheus, Pod B aus InfluxDB), kann Prometheus die Aggregation nicht leisten. Die Interpolation auf gemeinsame Zeitachsen ist dann essentiell. |
| **Empfehlung** | **Hybrid: Prometheus-Fastpath + Plugin-Fallback.** Wenn ALLE Pods eines Services aus derselben Prometheus-Instanz kommen (der Normalfall), kann die Aggregation an Prometheus delegiert werden. Falls Daten aus heterogenen Quellen stammen, bleibt die Plugin-Logik aktiv. **Dies hat das hoechste Performance-Potential.** |

### 2.2 Topological Aggregation (beliebige Knotenlisten)

| | |
|---|---|
| **Datei** | `topological_aggregation/TopologicalAggregation.java` |
| **Procedure** | `graphobs.aggregation.aggregate_nodes` |
| **Algorithmus** | Wie 2.1, aber ueber beliebige Knotenlisten (nicht nur Service-Pods) |
| **PromQL** | `sum(metric{pod=~"a|b|c"})` — per Regex-Filter moeglich |
| **Performance** | **++** — Gleiche Argumentation wie 2.1 |
| **Erweiterbarkeit** | Gleiche Einschraenkung: Nur wenn alle Knoten aus einer Prometheus-Instanz kommen. |
| **Empfehlung** | **Hybrid: Prometheus-Fastpath + Plugin-Fallback.** Wie 2.1. |

### 2.3 `uniteTimeSeries` (interne Aggregation in TimeSeriesUtil)

| | |
|---|---|
| **Datei** | `util/TimeSeriesUtil.java` (Zeile 220ff) |
| **Algorithmus** | Union aller Timestamps, lineare Interpolation, dann sum/mean |
| **PromQL** | Implizit in `sum()`/`avg()` |
| **Performance** | + |
| **Empfehlung** | **Plugin behalten** als interne Infrastruktur fuer den Fallback-Pfad. |

---

## 3. Statistische Analysen

### 3.1 Deskriptive Statistik (min, max, mean, median, stddev, percentile)

| | |
|---|---|
| **Datei** | `util/StatisticUtil.java` |
| **Algorithmus** | Berechnet min, max, mean, median, stddev, sum, count, Perzentile in einem Durchlauf |
| **PromQL** | Teilweise: `min()`, `max()`, `avg()`, `stddev()`, `quantile()` — aber nur als Aggregation ueber Label-Dimensionen, nicht ueber Zeitfenster |
| **Performance** | o — Berechnung auf bereits geholten Daten, O(n log n) wegen Sortierung |
| **Erweiterbarkeit** | Funktioniert quellenunabhaengig. |
| **Empfehlung** | **Plugin behalten.** Die Berechnung ist Post-Processing. |

### 3.2 T-Test: Zwei-Stichproben (unabhaengig)

| | |
|---|---|
| **Dateien** | `comparison/CompareTimeSeriesProcedure.java`, `CompareTwoGroupsProcedure.java`, `ComparePeriodsProcedure.java` |
| **Procedures** | `compare_ts_mean`, `compare_two_groups`, `compare_periods_means` |
| **Algorithmus** | Welch's T-Test via Apache Commons Math |
| **PromQL** | **Nicht vorhanden.** Prometheus hat keine statistischen Hypothesentests. |
| **Performance** | o — Die eigentliche T-Test-Berechnung ist O(n), der Engpass ist das Laden der Zeitreihen (durch Batch-Queries jetzt optimiert). |
| **Erweiterbarkeit** | Essentiell quellenunabhaengig: Vergleicht z.B. Pods auf verschiedenen Prometheus-Instanzen. |
| **Empfehlung** | **Plugin behalten.** Prometheus kann das nicht. |

### 3.3 T-Test: Gepaart

| | |
|---|---|
| **Datei** | `comparison/ComparePeriodsProcedure.java` |
| **Procedure** | `compare_periods_paired` |
| **Algorithmus** | Gepaarter T-Test (gleiche Stichprobengroesse erforderlich) |
| **PromQL** | Nicht vorhanden. |
| **Empfehlung** | **Plugin behalten.** |

---

## 4. Korrelationsanalysen

### 4.1 Pearson-Korrelation (zwei Zeitreihen)

| | |
|---|---|
| **Dateien** | `mathematical_relations/TimeSeriesCorrelation_II.java`, `NodeGroupCorrelation.java` |
| **Procedures** | `graphobs.analysis.correlation`, `graphobs.analysis.node_group_correlation` |
| **Algorithmus** | r = (n*sum(xy) - sum(x)*sum(y)) / sqrt(...) auf temporal-aligned Daten |
| **PromQL** | **Nicht vorhanden.** Prometheus hat keine Cross-Series-Korrelation. |
| **Performance** | o — Engpass ist Datenladen (jetzt batched), nicht Berechnung |
| **Erweiterbarkeit** | **Kernargument fuer das Plugin:** Korrelation zwischen Metriken aus verschiedenen Quellen (Prometheus + InfluxDB + OpenSearch) ist nur moeglich, wenn die Daten im Plugin zusammengefuehrt werden. |
| **Empfehlung** | **Plugin behalten.** Dies ist einer der Hauptgruende fuer die Plugin-Architektur. |

### 4.2 Kendall-Tau-Korrelation

| | |
|---|---|
| **Dateien** | `TimeSeriesCorrelation_II.java`, `NodeGroupCorrelation.java` |
| **Algorithmus** | Rangbasierte Korrelation: (concordant - discordant) / total |
| **PromQL** | Nicht vorhanden. |
| **Performance** | o (O(n^2), aber n ist typischerweise klein nach Resampling) |
| **Empfehlung** | **Plugin behalten.** |

### 4.3 Event-Value-Korrelation

| | |
|---|---|
| **Datei** | `mathematical_relations/EventCorrelationById.java` |
| **Procedure** | `graphobs.analysis.correlate_events_with_value_by_id` |
| **Algorithmus** | Binned Pearson-Korrelation zwischen Event-Haeufigkeit und Zeitreihen-Mittelwert |
| **PromQL** | Nicht vorhanden (Events sind kein Prometheus-Konzept). |
| **Empfehlung** | **Plugin behalten.** Events kommen aus Neo4j, nicht aus Prometheus. |

---

## 5. Temporal Joins (Zeitreihen-Alignment)

### 5.1 Linear Interpolation Join

| | |
|---|---|
| **Datei** | `temporal_joins/LinearInterpolationJoinStrategy.java` |
| **Algorithmus** | Union-Zeitachse im Ueberlappungsbereich, lineare Interpolation fehlender Werte |
| **PromQL** | Nicht vorhanden. Prometheus aligned intern auf `step`-Grenzen, bietet aber keinen expliziten Cross-Series-Join. |
| **Erweiterbarkeit** | **Essentiell.** Dies ist die Voraussetzung fuer alle quellenuebergreifenden Analysen (Korrelation, VAR/IRF). Ohne Temporal Joins koennen keine Zeitreihen aus verschiedenen Datenbanken miteinander verglichen werden. |
| **Empfehlung** | **Plugin behalten.** Fundamentale Infrastruktur. |

### 5.2 Forward Fill Join (LOCF)

| | |
|---|---|
| **Datei** | `temporal_joins/ForwardFillJoinStrategy.java` |
| **Algorithmus** | Letzter bekannter Wert wird vorwaerts getragen (Step-Funktion) |
| **PromQL** | `last_over_time(metric[range])` — aehnlich, aber nur fuer eine Serie |
| **Erweiterbarkeit** | Wichtig fuer diskrete/statusbasierte Daten (z.B. Deployment-Status). |
| **Empfehlung** | **Plugin behalten.** |

### 5.3 Resampling Join

| | |
|---|---|
| **Datei** | `temporal_joins/ResamplingJoinStrategy.java` |
| **Algorithmus** | Beide Serien auf festes Zeitraster aggregieren (Mittelwert pro Bucket), dann ueber gemeinsame Buckets joinen |
| **PromQL** | `avg_over_time(metric[interval])` pro Serie — aber der **Join** zweier resampleter Serien ist kein PromQL-Konzept. |
| **Erweiterbarkeit** | Essentiell fuer VAR-Modelle, die ein regelmaessiges Zeitgitter benoetigen. |
| **Empfehlung** | **Plugin behalten.** |

---

## 6. Fortgeschrittene Zeitreihen-Analyse

### 6.1 VAR(1) + Impulse Response Function

| | |
|---|---|
| **Dateien** | `mathematical_relations/VARImpulseResponse_II.java`, `VARImpulseNodeGroupProcedure.java` |
| **Procedures** | `graphobs.analysis.var_irf`, `graphobs.analysis.var_irf_node_group` |
| **Algorithmus** | VAR(1)-Modellschaetzung via OLS, Cholesky-Zerlegung der Residuen-Kovarianzmatrix, IRF-Berechnung ueber Matrixpotenzen |
| **PromQL** | **Nicht im Ansatz vorhanden.** Dies ist oekonometrische Modellierung, kein Monitoring. |
| **Performance** | o — Engpass ist Datenladen (jetzt batched). VAR(1)-Schaetzung selbst ist O(n). |
| **Empfehlung** | **Plugin behalten.** Keine TSDB bietet VAR-Modelle. |

### 6.2 PELT Changepoint Detection

| | |
|---|---|
| **Datei** | `time_series_analysis/ChangepointDetection_new.java` |
| **Procedure** | `graphobs.analysis.pelt` |
| **Algorithmus** | Pruned Exact Linear Time (PELT) mit dynamischer Programmierung, O(n) mit Pruning |
| **PromQL** | Nicht vorhanden. |
| **Empfehlung** | **Plugin behalten.** |

### 6.3 Outlier Detection (Regressions-Residuen)

| | |
|---|---|
| **Datei** | `time_series_analysis/OutlierDetection.java` |
| **Procedure** | `graphobs.analysis.detect_regression_outliers` |
| **Algorithmus** | Gleitfenster-Regression, Residuen > Schwellwert = Ausreisser |
| **PromQL** | Nicht vorhanden. `absent()` und `changes()` erkennen andere Anomalien. |
| **Empfehlung** | **Plugin behalten.** |

### 6.4 Event Impact Quantification

| | |
|---|---|
| **Datei** | `mathematical_relations/QuantifyEventImpact_II.java` |
| **Procedure** | `graphobs.analysis.quantify_event_impact` |
| **Algorithmus** | Baseline-Berechnung (mean + sigma*stddev), Peak-Impact und Impact-Duration nach Event |
| **PromQL** | Nicht vorhanden. |
| **Empfehlung** | **Plugin behalten.** Events kommen aus Neo4j. |

---

## Zusammenfassung: Entscheidungsmatrix

| # | Analyse | PromQL-Aequivalent | Perf.-Gewinn | Erweiterb.-Risiko | Empfehlung |
|---|---------|-------------------|-------------|-------------------|------------|
| 1.1 | Moving Average | Aehnlich (zeitbasiert) | o | Niedrig | **Plugin** |
| 1.2 | Binned Average | `avg_over_time()` | + | Niedrig | **Plugin** (ist Post-Processing) |
| 1.3 | Cumulative Sum | Keines | o | — | **Plugin** |
| 1.4 | Integral | Keines | o | — | **Plugin** |
| 1.5 | Difference | `delta()` (anders) | o | Niedrig | **Plugin** |
| 1.6 | Derivative | `rate()`/`deriv()` | + | Niedrig | **Plugin** (ist Post-Processing) |
| 1.7 | Linear Regression | `predict_linear()` (begrenzt) | o | — | **Plugin** |
| 2.1 | **Service Aggregation** | **`sum()`/`avg()`** | **++** | **Mittel** | **HYBRID** |
| 2.2 | **Topological Aggregation** | **`sum()`/`avg()` + Regex** | **++** | **Mittel** | **HYBRID** |
| 2.3 | uniteTimeSeries | Implizit | + | — | **Plugin** (intern) |
| 3.1 | Deskriptive Statistik | Teilweise | o | — | **Plugin** |
| 3.2 | T-Test (unabhaengig) | Keines | o | — | **Plugin** |
| 3.3 | T-Test (gepaart) | Keines | o | — | **Plugin** |
| 4.1 | Pearson-Korrelation | Keines | o | — | **Plugin** |
| 4.2 | Kendall-Tau | Keines | o | — | **Plugin** |
| 4.3 | Event-Value-Korrelation | Keines | o | — | **Plugin** |
| 5.1 | Linear Interpolation Join | Keines | o | — | **Plugin** |
| 5.2 | Forward Fill Join | Aehnlich | o | — | **Plugin** |
| 5.3 | Resampling Join | Aehnlich | o | — | **Plugin** |
| 6.1 | VAR(1) + IRF | Keines | o | — | **Plugin** |
| 6.2 | PELT Changepoint | Keines | o | — | **Plugin** |
| 6.3 | Outlier Detection | Keines | o | — | **Plugin** |
| 6.4 | Event Impact | Keines | o | — | **Plugin** |

---

## Strategische Empfehlung

### Was an Prometheus delegiert werden sollte (2 von 23 Analysen)

Nur **Multi-Serien-Aggregationen** (2.1 + 2.2) bieten signifikanten Performance-Gewinn:

**Prometheus kann serverseitig aggregieren** und uebertraegt nur 1 Ergebnis-Serie statt N Einzel-Serien.
Bei 50 Pods und 60s-Resolution ueber 10h sind das:

- **Aktuell:** 50 Serien x 600 Datenpunkte = 30.000 Datenpunkte uebertragen, dann im Plugin aggregiert
- **Mit Delegation:** 1 Serie x 600 Datenpunkte = 600 Datenpunkte uebertragen, bereits aggregiert

**Faktor ~50x weniger Datentransfer.**

#### Vorgeschlagene Implementierung: Prometheus-Fastpath

```
if (alle Nodes aus derselben Prometheus-Instanz) {
    // Fastpath: PromQL mit sum()/avg() + Regex-Filter
    sum(metric{pod=~"pod1|pod2|pod3"})
} else {
    // Fallback: Plugin-Aggregation (fuer heterogene Quellen)
    MultiSeriesAggregator.aggregate(...)
}
```

Dies ist im Wesentlichen eine Erweiterung der in A2 implementierten Batch-Logik.

### Was im Plugin bleiben muss (21 von 23 Analysen)

Alle anderen Analysen sollten im Plugin bleiben, weil:

1. **Kein PromQL-Aequivalent existiert** (16 von 23): Korrelationen, T-Tests, VAR/IRF, PELT, Outlier Detection, Event Impact, Cumulative Sum, Integral, Linear Regression
2. **Quellenunabhaengigkeit essentiell ist** (Temporal Joins): Die Joins sind die Grundlage fuer heterogene Analysen
3. **Der Performance-Gewinn marginal waere** (Einfache Aggregationen): Diese arbeiten auf bereits geholten Daten — die Berechnung selbst ist O(n) und nicht der Engpass
4. **Die Semantik abweicht** (Moving Average, Difference): Punktbasiert vs. zeitbasiert

### Fazit

Die Architekturentscheidung, Analysen im Plugin durchzufuehren, ist fuer **91% der Operationen korrekt und alternativlos**. Nur bei den Multi-Serien-Aggregationen (Service/Topological) lohnt sich ein optionaler Prometheus-Fastpath, der bereits durch die Batch-Query-Infrastruktur (A1/A2) vorbereitet ist. Die Erweiterung auf `sum()`/`avg()` in PromQL waere der naechste logische Schritt mit dem groessten verbleibenden Performance-Potential.
