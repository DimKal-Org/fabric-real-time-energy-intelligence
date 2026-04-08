# Fabric Demo — Real-Time Energy Intelligence on Microsoft Fabric
## Implementation Guide

---

## Overview

This guide walks through the complete implementation of an end-to-end real-time analytics demo. The scenario simulates an industrial manufacturing plant streaming energy consumption data from an Azure SQL Database — using Change Event Streaming (CES) — through Azure Event Hub into Microsoft Fabric, where it is surfaced via a **Real-time dashboard in Fabric** and a **Fabric data agent**.

The goal is _not to show another tool_. It is to demonstrate the capability and the real value that industrial manufacturing companies can get today — from machine sensor to boardroom decision in seconds, not months.

### Architecture Summary

```
[Source]
Azure SQL Database (Not exposed to public internet)
    └── Change Event Streaming (CES)
            └── Azure Event Hub — `energy-readings` (public endpoint)
                    └── Fabric Eventstream (energy-cg consumer group)
                            └── KQL Database — EnergyReadings table (raw ingested data from SQL CES)
                                            — ces_energy_readings_curated table + Update policy (deduplication)
                                            — energy_by_shift + Update policy (aggregation)

[Reference & Context — seeded once before demo]
Fabric Notebook (notebooks/00_seed_dimensions)
    └── ManufacturingLakehouse
            ├── dim_machine          (Delta table — machine metadata)
            ├── dim_energy_tariff    (Delta table — tariff lookup)
            └── carbon_intensity     (Delta table — 6 hrs synthetic carbon history)

[Insights]
KQL Database + ManufacturingLakehouse
    └── Fabric Real Time Dashboard     (KQL native queries)
    └── Fabric Data Agent             (queries BOTH KQL and Lakehouse)
```

## What the Demo Shows

### - **Phase 1 — Baseline (~0–2.5 min):** All 10 machines running normally. Clean, stable dashboard establishes trust with the audience.
### - **Phase 2 — Energy spike (~2.5 min):**       WELD-L2-A surges to 2.3× normal power draw. Cost-per-unit spikes visibly on the dashboard. Agent explains why.
### - **Phase 3 — OEE degradation (~4 min):**      COAT-L3-A drifts from 82% to ~61% OEE. Power stays flat but output drops — a silent financial bleed invisible without real-time data.
### - **Phase 4 — Both active (~4–7 min):**        Two simultaneous problems visible. Data agent answers compound questions across both anomalies, enriched with carbon context from the Lakehouse. After ~7 min the spike resolves — but the silent OEE bleed on COAT-L3-A continues, showing the harder-to-spot problem persists even after the obvious one clears.
### - **Phase 5 — Activator alert (on demand):**    Presenter flips a variable in the Variable Library (`spike_power_multiplier` → `2.3`). Dashboard spikes within 30 seconds. The Fabric Activator fires a Teams notification automatically — closing the loop from sensor to alert with zero code changes during the demo.

---

## Concepts Referenced in This Guide

### OEE — Overall Equipment Effectiveness

OEE is the standard manufacturing KPI that measures how much of your planned production time is truly productive. It is the product of three factors:

- **Availability** — Is the machine running when it should be? (accounts for downtime)
- **Performance** — Is it running at full speed? (accounts for slow cycles)
- **Quality** — Are the parts coming out good? (accounts for defects and rework)

A score of 100% means perfect. World-class manufacturers typically target around 85%. In this demo, OEE is the key signal for the degradation scenario: COAT-L3-A's power draw stays flat while OEE drifts down, meaning the machine is consuming the same energy but producing fewer good units. Cost-per-unit climbs silently with no visible alarm — the pattern that costs manufacturers millions annually without anyone noticing in time.

### Change Event Streaming (CES)

CES is a feature introduced in SQL Server 2025 that streams change events (INSERT, UPDATE, DELETE) directly from the SQL Server transaction log to Azure Event Hub in near real-time, without any intermediate broker or polling mechanism. CES replaces CDC for this demo — the two cannot coexist on the same database.

**Key requirement for SQL Server 2025:** CES is a preview feature. The database must also be in FULL recovery model.

**Important networking constraint:** CES can only stream to Azure Event Hub **public endpoints**. Private endpoints and VNet service endpoints are not currently supported. This means the Event Hub namespace must have public access enabled, secured with an IP firewall rule that allows only the VM's public IP.

### Why Gaussian Noise in the Simulator

The energy simulator applies Gaussian (normal distribution) noise of ±5% on power readings and ±2% on OEE. Real industrial sensors never produce perfectly stable readings — interference, vibration, and latency all introduce small random fluctuations. Without noise, a flat line on the dashboard immediately signals synthetic data to a technical audience. Gaussian noise produces the natural "breathing" pattern of real sensor data, making the baseline credible and anomalies stand out as genuinely unexpected.

### Why a Sigmoid Ramp for the Energy Spike

The energy spike on WELD-L2-A uses a sigmoid (S-curve) ramp rather than a step change. Real manufacturing equipment does not change state instantaneously — power surges ramp up as a machine transitions. A step change looks scripted; a sigmoid ramp looks like real machine behaviour. The ramp peaks at approximately 2.3× the baseline over about 90 seconds.

### Why Carbon Intensity in the Lakehouse

Carbon intensity (gCO₂/kWh) measures how much CO₂ was emitted per unit of electricity consumed based on the grid's current fuel mix. Combining it with energy consumption gives a carbon cost per unit of production — a KPI manufacturing sustainability teams face increasing regulatory pressure to track.

Carbon data is pre-seeded as a Delta table in the Lakehouse rather than streamed live. This avoids any external API dependency and gives the agent a full 4-hour history — including a renewable surge that happened 45–60 minutes ago — which enables far richer agent answers than a live feed that's barely a few minutes old.

The carbon data living in the **Lakehouse** (not the KQL database) is intentional. It demonstrates that the data agent can query across two different data stores — real-time KQL for energy readings and Delta tables for carbon context — in a single conversation. That is the unified analytics story.

### Why Tables with Update Policies Instead of Materialized Views
The KQL schema defines ces_energy_readings_curated and energy_by_shift as physical tables with update policies rather than materialized views. Both approaches produce the same result — automatic transformation of incoming data — but Fabric Data Agents can only query physical tables, not materialized views. Since the data agent is a core part of this demo, physical tables with update policies are required.

---

## Infrastructure Phase - Azure Resources
### I1. Create Azure Event Hub resource

1. In the Azure portal, create a new **Event Hubs namespace**. Standard tier is sufficient.
2. Create two Event Hubs (topics) inside the namespace:

| Event Hub namespace     | Partition count | Retention | Purpose                        |
|--------------------|-----------------|-----------|--------------------------------|
| `demo-fabric-agents-rti`  | 4               | 1 day     | Change Event Stream  from SQL Server|

3. For the Event Hub, create the following consumer groups:

| Event Hub          | Consumer group  | Used by                        |
|--------------------|-----------------|--------------------------------|
| `energy-readings`  | `energy-cg`     | Fabric Eventstream pipeline 1  |

4. Create a **Shared Access Policy** with `Send` + `Listen` permissions. Note the connection string — you will need it for the SQL configuration part..

### I2. Create Azure SQL Server and Database
1. In the Azure portal, create a new **Azure SQL Database**. Create a server if need be or host it to an existing server.

2. Enable access from selected networks temporarily to connect through SSMS and execute the `sql/setup.sql script`.

---

## Phase A — Azure SQL Database Setup
### A0 —  Setup database for CES

Connect to your SQL Server instance and run `Section 1` that appears on the sql script.

### A1 — Create the EnergyReadings table

Connect to your SQL Server instance and run `Section 2` that appears on the sql script.

### A2 — Create authentication for the Fabric notebook

The simulator notebook connects using either SQL Authentication or a Service Principal. Choose one approach and run the corresponding block. This is `Section 3` and `Section 4` of the sql script

> **Note for the demo:** SQL Authentication is simpler to configure and explain during a live session. Service Principal is more enterprise-grade. Either works identically for the notebook and CES connector. Store credentials in Azure Key Vault and reference them via Fabric environment secrets — never hardcode them in the notebook.

---

## Phase B — Fabric Workspace Setup

### B1 — Create the Fabric workspace and managed private endpoint

1. In the Fabric portal, create a new workspace. Assign an F SKU capacity (F2 minimum; F4 recommended for smooth streaming during a live demo).
2. Navigate to **Workspace Settings → Outbound Networking**.
3. Click **Create** and fill in:
   - Managed private endpoint name: `The name of your connection`
   - Resource identifier: `The resource ID must be formatted exactly as it appears as the primary resource ID for that resource in the Azure portal, starting with "/subscription/..."`
   - Target sub-resource: `Azure SQL Database`
4. Create the MPW. Go to your Azure SQL Server in the Azure portal → **Private endpoint connections** and **Approve** the pending request.
5. Wait 2–3 minutes for the endpoint to become active. Test connectivity from a Fabric notebook using the connection code in Phase D.

> **Demo talking point:** "The notebook never touches the public internet. Traffic from Fabric to SQL Server flows entirely over Microsoft's private backbone — this is how you'd connect to an on-premises or VNet-isolated SQL Server in a real enterprise environment, with no firewall exceptions needed."

### B2 — Create the Lakehouse and seed dimension tables

1. In the workspace, create a new **Lakehouse** named `manufacturing_lakehouse`.


### B3 — Create KQL database and table

1. In the Fabric workspace, create a new **Eventhouse** named `ManufacturingKQL`; this will also create a KQL database with the same name.
2. Run `kql/schema.kql` in the KQL query editor.

Objects created:

| Object | Type | Purpose |
|---|---|---|
| `EnergyReadings` | Table | Raw streaming — CES |
| `ces_energy_readings_curated` | Table + update policy | Deduplicates the raw data |
| `energy_by_shift` | Table + update policy | Aggregated per machine per shift |

### B4 — Create the Variable Library

All notebooks reference a shared Variable Library to resolve environment-specific paths (e.g. the Lakehouse ABFSS path) without hardcoding them.

1. In the Fabric workspace, create a new **Variable Library** named `var_library_rti`.
2. Add the following variables:

| Variable name             | Type   | Default value                                            | Purpose                                      |
|---------------------------|--------|----------------------------------------------------------|----------------------------------------------|
| `lakehouse_abfss`         | String | The ABFSS path of `manufacturing_lakehouse`              | Lakehouse path for notebooks                 |
| `spike_machine_id`        | String | *(empty string)*                                         | Machine to target with spike — set during demo |
| `spike_power_multiplier`  | String | `1.0`                                                    | Power multiplier — set to `2.3` to trigger spike |

> To find the ABFSS path: open the Lakehouse, click **…** → **Properties** → copy the **ABFSS path**.

Notebooks load it with:
```python
variables = notebookutils.variableLibrary.getLibrary("var_library_rti")
# then use: variables.lakehouse_abfss
```
Why a Variable Library? It decouples notebooks from a specific workspace or Lakehouse instance. You can clone the workspace, update a single variable, and all notebooks work — no find-and-replace across code cells.
---

## Phase C — Eventstream configuration for energy readings

1. In Fabric `Manage connections and gateways` page create a new `Cloud` connection pointing to the Event Hub Azure reosurce. Use SAS Key for authentication.
2. In the Fabric workspace, create a new **Eventstream** named `EnergyReadingsStream`.
3. **Source:** Azure Event Hub
   - Namespace: `demo-fabric-agents-rti`
   - Event Hub: `energy-readings`
   - Consumer group: `energy-cg`
   - Authentication: Shared Access Policy (connection string from I1)
4. **Destination:** KQL Database
   - Workspace: your Fabric workspace
   - KQL Database: `ManufacturingKQL`
   - Table: `EnergyReadings`
5. Save and **Publish** the Eventstream. Confirm the status shows **Running**.

---

## Phase D — Notebooks

### D1 — Carbon Intensity notebook (run once)

Run the notebook `notebooks/01_carbon_intensity.ipynb`. No configuration needed. Re-running safely overwrites data.

### D2 - Seed Dimensions notebook (run once)

Run the notebook `notebooks/00_seed_dimensions.ipynb`.

> For the demo, use `TARIFF-DEMO` (flat €0.14/kWh) to keep cost calculations simple and explainable. In a real deployment you would join against peak/off-peak tariffs dynamically.

### D3 — Energy simulator notebook

This is the main simulator. Connect to SQL Server using your chosen authentication method and run the continuous loop.

Run the notebook `notebooks/02_energy_simulator.ipynb`.
---

## Phase E — Insights Layer

### E1 — Fabric Real Time Dashboard

Create a new **Real Time Dashboard** in the Fabric workspace connected to the `ManufacturingKQL` KQL database. Real Time Dashboards query KQL natively and support automatic refresh — no DirectQuery or import mode needed.

1. In the workspace, click **+ New → Real Time Dashboard**.
2. Name it `Energy Intelligence Dashboard`.
3. Add the `ManufacturingKQL` database as a data source.
4. Create tiles using the KQL queries below. All queries target the deduplicated `ces_energy_readings_curated` table or the pre-aggregated `energy_by_shift` table — never the raw `EnergyReadings` table.
5. Set the dashboard **auto-refresh interval to 30 seconds** (minimum supported).

Recommended tiles and their KQL queries:

**Cost per unit — per machine (last 10 minutes)**
```kql
ces_energy_readings_curated
| where Timestamp > ago(10m)
| summarize AvgCostPerUnit = avg(CostPerUnit) by MachineId
| order by AvgCostPerUnit desc
```

**Live energy draw — all machines**
```kql
ces_energy_readings_curated
| where Timestamp > ago(5m)
| summarize AvgPowerKw = avg(PowerKw) by MachineId, bin(Timestamp, 30s)
| order by Timestamp asc
```

**OEE trend — last 15 minutes**
```kql
ces_energy_readings_curated
| where Timestamp > ago(15m)
| summarize AvgOee = avg(OeePercent) by MachineId, bin(Timestamp, 1m)
```

**Shift summary — current shift (from pre-aggregated table)**
```kql
energy_by_shift
| where ShiftStart > ago(8h)
| project MachineId, LineId, ShiftId, TotalCostEur, TotalKwh, AvgOee, AvgCostPerUnit, TotalUnits, PeakPowerKw
| order by TotalCostEur desc
```

**Cumulative shift cost ticker**
```kql
energy_by_shift
| where ShiftStart > ago(8h)
| summarize TotalCostEur = sum(TotalCostEur), TotalKwh = sum(TotalKwh), TotalUnits = sum(TotalUnits)
```

**Suggested visual types per tile:**

| Tile | Visual type | Notes |
|---|---|---|
| Cost per unit — per machine | Bar chart (horizontal) | Sort descending so the worst offender is on top |
| Live energy draw — all machines | Line chart | One line per `MachineId`, `Timestamp` on x-axis — this is the "moving data" tile |
| OEE trend — last 15 minutes | Line chart or Area chart | Area variant makes OEE drops more dramatic (useful for the COAT-L3-A degradation story) |
| Shift summary — current shift | Table | Multiple columns per machine — no single chart captures all dimensions |
| Cumulative shift cost ticker | Stat (multi-stat card) | Three big numbers: total cost €, total kWh, total units |

### E2 — Fabric Activator

Create a **Data Activator** to automatically alert when a machine's cost per unit exceeds its normal range.

1. In the Fabric workspace, click **+ New → Activator**.
2. **Source:** KQL Database → `ces_energy_readings_curated`
3. **Object ID:** Set `MachineId` as the object identifier (each machine is monitored independently).
4. **Condition:** `CostPerUnit > 0.003` (baseline machines sit around €0.001–0.002 per unit; a 2.3× spike pushes it well above this threshold).
5. **Action:** Send a **Teams message** or **Email** — e.g., *"Machine {MachineId} cost per unit exceeded threshold: €{CostPerUnit}"*

**Demo flow:**
1. Simulator running → dashboard shows stable baseline → Activator is silent.
2. Presenter opens Variable Library → sets `spike_machine_id` = `WELD-L2-A` and `spike_power_multiplier` = `2.3`.
3. Next simulator tick picks it up → power surges → CES → Event Hub → KQL.
4. Dashboard visibly spikes within 30 seconds.
5. Activator fires → Teams/email notification arrives.
6. Presenter resets `spike_power_multiplier` = `1.0` → machine returns to normal → alert clears.

> **Demo talking point:** "No code was changed. No notebook was restarted. A single configuration change in the Variable Library caused a real data event that flowed through the entire pipeline — from SQL Server to Event Hub to KQL to an automated alert — in under a minute."

### E3 — Fabric data agent

1. In the Fabric workspace, create a new **Data Agent**.
2. Add data sources:
   - KQL Database: `ManufacturingKQL` (tables: `ces_energy_readings_curated`, `energy_by_shift`)
   - Lakehouse: `ManufacturingLakehouse` (tables: `dim_machine`, `dim_energy_tariff`, `carbon_intensity`)
3. Set the following system prompt:

```
You are an industrial energy intelligence assistant for a manufacturing plant.
You have access to real-time energy consumption data, OEE metrics, carbon intensity,
and machine reference data.

When answering questions:
- Always express financial impact in EUR and round to 2 decimal places
- Express carbon intensity in gCO₂/kWh and carbon cost in gCO₂ per unit produced
- When identifying anomalies, name the specific machine and line
- Compare current performance against the machine's nominal power (from dim_machine)
- Keep answers concise — one or two sentences followed by the key numbers

You are talking to a plant manager or operations director. Be direct and actionable.
```

4. Suggested demo Q&A flows to rehearse (ordered by escalating reasoning complexity):

| # | Question | What it demonstrates |
|---|---|---|---|
| 1 | Which machine is currently consuming power significantly above its nominal rating, and what event might explain it?	| Anomaly detection via cross-table join (live readings vs. dim_machine.NominalPowerKw) and causal correlation with EventTag |
| 2 | Is there a machine where OEE has been declining while energy cost per unit has been rising? What does that suggest about its operational health? | Inverse trend detection across two metrics over time; business interpretation of signal correlation |
| 3 | Which shift produces the most units per euro spent, and does that advantage come from lower tariffs or better OEE? | Derived ratio reasoning (TotalUnits / TotalCostEur) with causal attribution across energy_by_shift and dim_energy_tariff |
| 4 | Which critical machines are past their maintenance cycle and also showing above-average power draw? Should I be concerned? | Risk assessment joining dim_machine (CriticalityRating, MaintenanceCycleDays, InstallYear) with real-time consumption patterns |
| 5 | Over the last hour, did any machine experience a sudden power spike followed by an OEE drop within the next few minutes? | Describe the sequence of events.	Temporal pattern detection — windowed time-series analysis with event sequencing and narrative explanation |
| 6 | What percentage of today's total energy cost is attributable to anomalous events (spikes or degradation) versus normal operation? | Cost decomposition by filtering on EventTag presence; quantifying the financial impact of anomalies |
| 7 | Compare Line2-Weld and Line4-Assembly: which line is more energy-efficient per production unit, and what machine characteristics from the dimension table explain the difference? | Multi-table benchmarking with causal explanation — aggregated KQL metrics joined to dim_machine attributes (MachineType, NominalPowerKw, InstallYear, BaseOEE) |

---

## Demo Day Checklist

### 30 minutes before

- [ ] Start **D1** (carbon intensity notebook) — confirm rows appearing in `carbon_intensity` lakehouse table
- [ ] Start **D3** (energy simulator notebook) — confirm rows appearing in `EnergyReadings` KQL table
- [ ] Open Real Time Dashboard — confirm tiles refreshing with live data
- [ ] Open data agent — run one test question to confirm it responds correctly
- [ ] Check Eventstream status — should show **Running**
- [ ] Confirm Activator is active and condition is set (`CostPerUnit > 0.003`)
- [ ] Confirm Variable Library has `spike_machine_id` = *(empty)* and `spike_power_multiplier` = `1.0`

### During the demo

- Simulator runs automatically through Phases 1–4 — no manual intervention needed
- Phase 2 (energy spike) begins at approximately **2.5 minutes** after D3 starts
- Phase 3 (OEE degradation) begins at approximately **4 minutes** after D3 starts
- Both anomalies are sustained from **4 minutes** onwards — this is when to ask the compound agent questions
- **Phase 5 (Activator):** When ready, open Variable Library → set `spike_machine_id` = `WELD-L2-A` and `spike_power_multiplier` = `2.3` → wait for dashboard spike → Teams notification arrives → reset `spike_power_multiplier` = `1.0`

### If something goes wrong

- **No data on dashboard:** Check Eventstream status. If stopped, restart the pipeline. Data resumes within 30 seconds.
- **Notebook disconnected:** Restart D3. The simulator picks up from the current iteration — cumulative kWh will reset but the demo phases will replay correctly.
- **Agent not responding:** Refresh the agent page. KQL queries run fresh on each question so there is no stale state to clear.

---

## Repository Structure

```
fabric-rti/
├── README.md                        ← this file
├── sql/
│   └── setup.sql                    ← Phase A scripts (DDL + CES + auth)
├── notebooks/
│   ├── 00_seed_dimensions.ipynb        ← Phase B2: Lakehouse dimension tables
│   ├── 01_carbon_intensity.ipynb       ← Phase D1: carbon Intensity → Lakehouse
│   └── 02_energy_simulator.ipynb       ← Phase D3: simulator → SQL Server
└── kql/
    └── schema.kql                   ← Phase B4: all KQL table and view definitions
```
