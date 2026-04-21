<h2 id="sql-section">Advanced SQL Workflow: Building and Querying a Resilience Measurement Warehouse</h2>

<p>The SQL workflow below adds a structured data layer to resilience measurement. This is useful when resilience indicators, disturbance records, recovery metrics, adaptive-capacity variables, and threshold-risk flags need to be stored, joined, and queried across places, sectors, or time periods. In practice, SQL is especially relevant for resilience metrics because many assessment systems depend on reproducible indicator pipelines, auditable scorecards, and cross-table comparisons rather than one-off calculations.</p>

<p>This workflow uses a relational structure to connect systems, indicators, disturbances, observed performance, and threshold signals. The point is not to replace mathematical, R, or Python analysis. It is to provide a durable query layer that supports dashboards, reporting, scenario comparison, and longitudinal resilience assessment.</p>

<pre><code class="language-sql">-- ============================================================
-- SQL Workflow: Building and Querying a Resilience Measurement Warehouse
-- Purpose:
--   Store resilience indicators, disturbance events, recovery outcomes,
--   adaptive-capacity metrics, and threshold warnings in a structured,
--   queryable format for cross-system analysis.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Core system registry
-- ------------------------------------------------------------ø
CREATE TABLE systems (
    system_id INTEGER PRIMARY KEY,
    system_name VARCHAR(150) NOT NULL,
    system_type VARCHAR(100) NOT NULL,     -- e.g. infrastructure, ecosystem, community, institution
    region_name VARCHAR(150) NOT NULL,
    scale_level VARCHAR(50) NOT NULL,      -- e.g. local, regional, national
    essential_function TEXT NOT NULL,
    created_at DATE NOT NULL
);

-- ------------------------------------------------------------
-- 2. Indicator catalog
-- ------------------------------------------------------------
CREATE TABLE resilience_indicators (
    indicator_id INTEGER PRIMARY KEY,
    indicator_name VARCHAR(150) NOT NULL,
    indicator_domain VARCHAR(100) NOT NULL,   -- e.g. resistance, recovery, adaptive_capacity, threshold_risk
    indicator_unit VARCHAR(50),
    higher_is_better BOOLEAN NOT NULL,
    description TEXT
);

-- ------------------------------------------------------------
-- 3. Indicator observations by system and time
-- ------------------------------------------------------------
CREATE TABLE system_indicator_observations (
    observation_id INTEGER PRIMARY KEY,
    system_id INTEGER NOT NULL,
    indicator_id INTEGER NOT NULL,
    observation_date DATE NOT NULL,
    indicator_value DECIMAL(12,4) NOT NULL,
    data_source VARCHAR(200),
    notes TEXT,
    FOREIGN KEY (system_id) REFERENCES systems(system_id),
    FOREIGN KEY (indicator_id) REFERENCES resilience_indicators(indicator_id)
);

-- ------------------------------------------------------------
-- 4. Disturbance events
-- ------------------------------------------------------------
CREATE TABLE disturbance_events (
    event_id INTEGER PRIMARY KEY,
    event_name VARCHAR(200) NOT NULL,
    event_type VARCHAR(100) NOT NULL,       -- e.g. flood, outage, wildfire, market shock, drought
    event_start_date DATE NOT NULL,
    event_end_date DATE,
    severity_score DECIMAL(8,2) NOT NULL,
    region_name VARCHAR(150) NOT NULL,
    description TEXT
);

-- ------------------------------------------------------------
-- 5. System performance during and after disturbances
-- ------------------------------------------------------------
CREATE TABLE system_disturbance_performance (
    performance_id INTEGER PRIMARY KEY,
    system_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    function_loss_pct DECIMAL(6,2) NOT NULL,      -- percent loss of function at peak disruption
    recovery_days INTEGER,                        -- time to restore essential function
    post_event_adaptation_score DECIMAL(8,2),     -- score for adaptation or reorganization quality
    threshold_warning_flag BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    FOREIGN KEY (system_id) REFERENCES systems(system_id),
    FOREIGN KEY (event_id) REFERENCES disturbance_events(event_id)
);

-- ------------------------------------------------------------
-- 6. Threshold and early warning signals
-- ------------------------------------------------------------
CREATE TABLE threshold_signals (
    signal_id INTEGER PRIMARY KEY,
    system_id INTEGER NOT NULL,
    signal_date DATE NOT NULL,
    signal_type VARCHAR(100) NOT NULL,       -- e.g. rising_variance, slower_recovery, increasing_autocorrelation
    signal_strength DECIMAL(8,4) NOT NULL,
    interpretation TEXT,
    FOREIGN KEY (system_id) REFERENCES systems(system_id)
);

-- ------------------------------------------------------------
-- 7. Sample systems
-- ------------------------------------------------------------
INSERT INTO systems (system_id, system_name, system_type, region_name, scale_level, essential_function, created_at)
VALUES
(1, 'Metro Water Network', 'infrastructure', 'Central Region', 'regional', 'Provide safe water continuity', DATE '2026-01-01'),
(2, 'Coastal Wetland Complex', 'ecosystem', 'Delta Coast', 'regional', 'Buffer flooding and sustain habitat function', DATE '2026-01-01'),
(3, 'Urban Health System', 'institution', 'North City', 'local', 'Maintain emergency and primary care capacity', DATE '2026-01-01'),
(4, 'Community Food Access Network', 'community', 'South District', 'local', 'Sustain food access during disruption', DATE '2026-01-01');

-- ------------------------------------------------------------
-- 8. Sample indicators
-- ------------------------------------------------------------
INSERT INTO resilience_indicators (indicator_id, indicator_name, indicator_domain, indicator_unit, higher_is_better, description)
VALUES
(1, 'Resistance Score', 'resistance', 'index', TRUE, 'Capacity to absorb shock before major degradation'),
(2, 'Recovery Speed', 'recovery', 'days_inverse_index', TRUE, 'Higher score means faster recovery'),
(3, 'Adaptive Capacity Score', 'adaptive_capacity', 'index', TRUE, 'Learning, flexibility, and response capacity'),
(4, 'Buffer Capacity Score', 'buffering', 'index', TRUE, 'Reserve capacity and redundancy'),
(5, 'Threshold Risk Score', 'threshold_risk', 'index', FALSE, 'Higher values indicate closer proximity to dangerous thresholds');

-- ------------------------------------------------------------
-- 9. Sample indicator observations
-- ------------------------------------------------------------
INSERT INTO system_indicator_observations (
    observation_id, system_id, indicator_id, observation_date, indicator_value, data_source, notes
)
VALUES
(1, 1, 1, DATE '2026-03-01', 78.4, 'utility_assessment', 'Water network resistance before flood season'),
(2, 1, 2, DATE '2026-03-01', 71.2, 'utility_assessment', 'Recovery capability baseline'),
(3, 1, 3, DATE '2026-03-01', 69.8, 'governance_review', 'Adaptive planning capacity'),
(4, 1, 4, DATE '2026-03-01', 74.5, 'asset_inventory', 'Reserve pumping and storage capacity'),
(5, 1, 5, DATE '2026-03-01', 34.7, 'risk_model', 'Threshold pressure moderate'),

(6, 2, 1, DATE '2026-03-01', 82.1, 'ecosystem_monitoring', 'Wetland shock absorption score'),
(7, 2, 3, DATE '2026-03-01', 76.4, 'ecosystem_monitoring', 'Adaptive ecological capacity'),
(8, 2, 5, DATE '2026-03-01', 41.6, 'threshold_assessment', 'Threshold pressure elevated'),

(9, 3, 1, DATE '2026-03-01', 73.9, 'health_system_review', 'Service resistance baseline'),
(10, 3, 2, DATE '2026-03-01', 67.5, 'health_system_review', 'Recovery speed baseline'),
(11, 3, 3, DATE '2026-03-01', 72.2, 'governance_review', 'Adaptive response capability'),

(12, 4, 1, DATE '2026-03-01', 68.7, 'community_assessment', 'Food access resistance baseline'),
(13, 4, 3, DATE '2026-03-01', 75.9, 'community_assessment', 'Adaptive community capacity'),
(14, 4, 4, DATE '2026-03-01', 64.3, 'community_assessment', 'Buffering through alternate supply channels');

-- ------------------------------------------------------------
-- 10. Sample disturbance events
-- ------------------------------------------------------------
INSERT INTO disturbance_events (
    event_id, event_name, event_type, event_start_date, event_end_date, severity_score, region_name, description
)
VALUES
(1, 'Spring Flood 2026', 'flood', DATE '2026-04-10', DATE '2026-04-16', 8.4, 'Central Region', 'Major regional flooding with transport and utility impacts'),
(2, 'Extreme Heatwave 2026', 'heatwave', DATE '2026-07-02', DATE '2026-07-09', 7.8, 'North City', 'Heat stress affecting power, health, and labor systems'),
(3, 'Drought Escalation 2026', 'drought', DATE '2026-05-01', DATE '2026-08-30', 8.1, 'Delta Coast', 'Prolonged drought affecting ecological and water conditions');

-- ------------------------------------------------------------
-- 11. Sample disturbance performance records
-- ------------------------------------------------------------
INSERT INTO system_disturbance_performance (
    performance_id, system_id, event_id, function_loss_pct, recovery_days, post_event_adaptation_score, threshold_warning_flag, notes
)
VALUES
(1, 1, 1, 32.5, 9, 74.2, FALSE, 'Water service partially restored within nine days'),
(2, 3, 2, 28.1, 6, 77.6, FALSE, 'Health system under heavy heat stress but recovered'),
(3, 2, 3, 21.4, 45, 68.8, TRUE, 'Wetland system showing threshold warning under prolonged drought'),
(4, 4, 1, 37.8, 12, 72.4, FALSE, 'Food access disruption mitigated by alternate local channels');

-- ------------------------------------------------------------
-- 12. Sample threshold signals
-- ------------------------------------------------------------
INSERT INTO threshold_signals (
    signal_id, system_id, signal_date, signal_type, signal_strength, interpretation
)
VALUES
(1, 2, DATE '2026-08-15', 'slower_recovery', 0.74, 'Wetland recovery is slowing relative to prior periods'),
(2, 2, DATE '2026-08-20', 'rising_variance', 0.68, 'Ecological volatility increasing'),
(3, 1, DATE '2026-04-20', 'increasing_autocorrelation', 0.42, 'Moderate persistence in service instability'),
(4, 4, DATE '2026-04-18', 'slower_recovery', 0.39, 'Community food access recovered but unevenly');

-- ============================================================
-- ANALYTICAL QUERIES
-- ============================================================

-- ------------------------------------------------------------
-- Query 1: Latest resilience indicator profile by system
-- Purpose:
--   Retrieve the most recent observed value for each resilience indicator
--   for each system.
-- ------------------------------------------------------------
WITH latest_observations AS (
    SELECT
        sio.system_id,
        sio.indicator_id,
        MAX(sio.observation_date) AS latest_date
    FROM system_indicator_observations sio
    GROUP BY sio.system_id, sio.indicator_id
)
SELECT
    s.system_name,
    ri.indicator_name,
    sio.indicator_value,
    sio.observation_date
FROM latest_observations lo
JOIN system_indicator_observations sio
    ON lo.system_id = sio.system_id
   AND lo.indicator_id = sio.indicator_id
   AND lo.latest_date = sio.observation_date
JOIN systems s
    ON s.system_id = sio.system_id
JOIN resilience_indicators ri
    ON ri.indicator_id = sio.indicator_id
ORDER BY s.system_name, ri.indicator_name;

-- ------------------------------------------------------------
-- Query 2: Systems with highest threshold risk
-- Purpose:
--   Identify systems with elevated threshold-risk scores based on the
--   most recent threshold-risk indicator.
-- ------------------------------------------------------------
WITH latest_threshold_risk AS (
    SELECT
        sio.system_id,
        MAX(sio.observation_date) AS latest_date
    FROM system_indicator_observations sio
    WHERE sio.indicator_id = 5
    GROUP BY sio.system_id
)
SELECT
    s.system_name,
    s.system_type,
    s.region_name,
    sio.indicator_value AS threshold_risk_score
FROM latest_threshold_risk ltr
JOIN system_indicator_observations sio
    ON ltr.system_id = sio.system_id
   AND ltr.latest_date = sio.observation_date
   AND sio.indicator_id = 5
JOIN systems s
    ON s.system_id = sio.system_id
ORDER BY threshold_risk_score DESC;

-- ------------------------------------------------------------
-- Query 3: Average recovery time by system type
-- Purpose:
--   Compare post-disturbance recovery performance across system types.
-- ------------------------------------------------------------
SELECT
    s.system_type,
    AVG(sdp.recovery_days) AS avg_recovery_days,
    AVG(sdp.function_loss_pct) AS avg_function_loss_pct
FROM system_disturbance_performance sdp
JOIN systems s
    ON s.system_id = sdp.system_id
GROUP BY s.system_type
ORDER BY avg_recovery_days ASC;

-- ------------------------------------------------------------
-- Query 4: Disturbance events that triggered threshold warnings
-- Purpose:
--   Surface high-risk events associated with warning flags.
-- ------------------------------------------------------------
SELECT
    s.system_name,
    de.event_name,
    de.event_type,
    de.severity_score,
    sdp.function_loss_pct,
    sdp.recovery_days,
    sdp.post_event_adaptation_score
FROM system_disturbance_performance sdp
JOIN systems s
    ON s.system_id = sdp.system_id
JOIN disturbance_events de
    ON de.event_id = sdp.event_id
WHERE sdp.threshold_warning_flag = TRUE
ORDER BY de.severity_score DESC, sdp.recovery_days DESC;

-- ------------------------------------------------------------
-- Query 5: Composite resilience reporting view
-- Purpose:
--   Build a view combining latest indicator signals into one reporting layer.
-- ------------------------------------------------------------
CREATE VIEW resilience_reporting_view AS
SELECT
    s.system_id,
    s.system_name,
    s.system_type,
    s.region_name,
    MAX(CASE WHEN ri.indicator_name = 'Resistance Score' THEN sio.indicator_value END) AS resistance_score,
    MAX(CASE WHEN ri.indicator_name = 'Recovery Speed' THEN sio.indicator_value END) AS recovery_speed_score,
    MAX(CASE WHEN ri.indicator_name = 'Adaptive Capacity Score' THEN sio.indicator_value END) AS adaptive_capacity_score,
    MAX(CASE WHEN ri.indicator_name = 'Buffer Capacity Score' THEN sio.indicator_value END) AS buffer_capacity_score,
    MAX(CASE WHEN ri.indicator_name = 'Threshold Risk Score' THEN sio.indicator_value END) AS threshold_risk_score
FROM systems s
LEFT JOIN system_indicator_observations sio
    ON s.system_id = sio.system_id
LEFT JOIN resilience_indicators ri
    ON ri.indicator_id = sio.indicator_id
GROUP BY
    s.system_id,
    s.system_name,
    s.system_type,
    s.region_name;

-- Use the reporting view.
SELECT *
FROM resilience_reporting_view
ORDER BY threshold_risk_score DESC NULLS LAST, adaptive_capacity_score DESC NULLS LAST;

-- ------------------------------------------------------------
-- Query 6: Join disturbance history with early warning signals
-- Purpose:
--   Assess whether threshold warnings align with later disruption.
-- ------------------------------------------------------------
SELECT
    s.system_name,
    ts.signal_date,
    ts.signal_type,
    ts.signal_strength,
    de.event_name,
    de.event_start_date,
    de.event_type,
    de.severity_score
FROM threshold_signals ts
JOIN systems s
    ON s.system_id = ts.system_id
LEFT JOIN system_disturbance_performance sdp
    ON sdp.system_id = ts.system_id
LEFT JOIN disturbance_events de
    ON de.event_id = sdp.event_id
   AND de.event_start_date >= ts.signal_date
ORDER BY s.system_name, ts.signal_date, de.event_start_date;

-- ------------------------------------------------------------
-- Query 7: Rank systems by balanced resilience profile
-- Purpose:
--   Produce a simple reporting score using multiple indicator domains.
--   This is not a universal truth score; it is a transparent reporting aid.
-- ------------------------------------------------------------
SELECT
    system_name,
    system_type,
    region_name,
    ROUND(
        (
            COALESCE(resistance_score, 0) * 0.25 +
            COALESCE(recovery_speed_score, 0) * 0.20 +
            COALESCE(adaptive_capacity_score, 0) * 0.25 +
            COALESCE(buffer_capacity_score, 0) * 0.20 -
            COALESCE(threshold_risk_score, 0) * 0.10
        ),
        2
    ) AS balanced_resilience_reporting_score
FROM resilience_reporting_view
ORDER BY balanced_resilience_reporting_score DESC;
</code></pre>

<p>This SQL layer shows how resilience metrics can be operationalized as a structured monitoring and reporting system. Rather than treating resilience as a vague label, the database design makes the framework auditable: systems are defined explicitly, indicators are tied to dates and sources, disturbances are logged, recovery outcomes are queryable, and threshold signals can be linked to later events. That makes SQL especially useful for resilience analysis because it supports the kind of traceable, longitudinal, and cross-sector assessment that dashboards and institutional reporting often require.</p>
