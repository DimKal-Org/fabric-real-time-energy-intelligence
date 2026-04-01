# Fabric Demo — Real-Time Energy Intelligence on Microsoft Fabric
## Implementation Guide

---

## Overview

This guide walks through the complete implementation of an end-to-end real-time analytics demo. The scenario simulates an industrial manufacturing plant streaming energy consumption data from an Azure SQL Database — using Change Event Streaming (CES) — through Azure Event Hub into Microsoft Fabric, where it is surfaced via a **Power BI real-time dashboard** and a **Fabric data agent**.

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
    └── Power BI Real-Time Dashboard  (Direct Query from KQL)
    └── Fabric Data Agent             (queries BOTH KQL and Lakehouse)
```

### What the Demo Shows

- **Phase 1 — Baseline (~0–1.5 min):** All 10 machines running normally. Clean, stable dashboard establishes trust with the audience.
- **Phase 2 — Energy spike (~1.5 min):** WELD-L2-A surges to 2.3× normal power draw. Cost-per-unit spikes visibly on the dashboard. Agent explains why.
- **Phase 3 — OEE degradation (~2.5 min):** COAT-L3-A drifts from 82% to ~61% OEE. Power stays flat but output drops — a silent financial bleed invisible without real-time data.
- **Phase 4 — Both sustained (~5 min onward):** Two simultaneous problems visible. Data agent answers compound questions across both anomalies, enriched with carbon context from the Lakehouse.

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

```sql
-- Step 1: Create a dedicated database
CREATE DATABASE [sql-fabric-rti];
GO

-- Step 2: Create master key in the newly created database
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<Your Master Key Password>'
GO
-- IMPORTANT: Make sure you have created a `Shared Access policy` first on the Event Hub instance
CREATE DATABASE SCOPED CREDENTIAL EventHubsCreds
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '<SAS_TOKEN_FOR_ENERGY_READINGS_EVENT_HUB>' -- Should start like Endpoint=sb://...
GO

-- Step 3: Enable the change event stream
EXEC sys.sp_enable_event_stream
GO

-- Step 4: Create a change event stream group
EXEC sys.sp_create_event_stream_group
    @stream_group_name =      N'EnergyReadingsStreamGroup',
    @destination_type =       N'AzureEventHubsApacheKafka',
    @destination_location =   N'<myEventHubsNamespace>.servicebus.windows.net:9093/<myEventHubsInstance>',
    @destination_credential = EventHubsCreds,
    @encoding = N'JSON',
    @max_message_size_kb =    256;
    --@partition_key_scheme =   N'<PatitionKeyScheme>'
GO
```

### A1 — Create the EnergyReadings table and enable CDC

Connect to your SQL Server instance and run the following script. CDC must be enabled at the database level before it can be enabled at the table level.

```sql
-- Step 1: Create the EnergyReadings table
CREATE TABLE dbo.EnergyReadings (
    ReadingId        UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    Timestamp        DATETIME2(3)     NOT NULL,
    MachineId        NVARCHAR(50)     NOT NULL,
    PlantId          NVARCHAR(50)     NOT NULL DEFAULT 'PLANT-GR-01',
    LineId           NVARCHAR(50)     NOT NULL,
    ShiftId          NVARCHAR(20)     NOT NULL,
    PowerKw          FLOAT            NOT NULL,
    EnergyKwhCumul   FLOAT            NOT NULL,
    OeePercent       FLOAT            NOT NULL,
    ProductionUnits  BIGINT           NOT NULL,
    CostPerKwh       FLOAT            NOT NULL,
    CostThisTick     FLOAT            NOT NULL,
    CostPerUnit      FLOAT            NOT NULL,
    EventTag         NVARCHAR(50)     NULL
);
GO

-- Step 2: Add the table to the event stream group
EXEC sys.sp_add_object_to_event_stream_group
    N'EnergyReadingsStreamGroup',
    N'dbo.EnergyReadings'
```

### A2 — Create authentication for the Fabric notebook

The simulator notebook connects using either SQL Authentication or a Service Principal. Choose one approach and run the corresponding block.

**Option A — SQL Authentication (simplest for demo)**

```sql
-- Create a SQL login at the server level
USE [master];
CREATE LOGIN sql_user WITH PASSWORD = 'YourStr0ngPassword!';
GO

-- Create a database user mapped to the login
USE [sql-fabric-rti];
CREATE USER sql_user FROM LOGIN sql_user;
GO

-- Grant read/write access
ALTER ROLE db_datareader ADD MEMBER sql_user;
GO

ALTER ROLE db_datawriter ADD MEMBER sql_user;
GO
```

**Option B — Service Principal Authentication**

```sql
-- First, register an App Registration in Azure AD and note:
--   Tenant ID, Client ID, Client Secret

-- Create an external user in the database mapped to the Service Principal
USE [sql-fabric-rti];
CREATE USER [your-app-registration-name] FROM EXTERNAL PROVIDER;
GO

ALTER ROLE db_datareader ADD MEMBER [your-app-registration-name];
ALTER ROLE db_datawriter ADD MEMBER [your-app-registration-name];
```

> **Note for the demo:** SQL Authentication is simpler to configure and explain during a live session. Service Principal is more enterprise-grade. Either works identically for the notebook and CDC connector. Store credentials in Azure Key Vault and reference them via Fabric environment secrets — never hardcode them in the notebook.

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

---

## Phase C — Eventstream configuration for energy readings

1. In Fabric `Manage connections and gateways` page create a new `Cloud` connection pointing to the Event Hub Azure reosurce. Use SAS Key for authentication.
2. In the Fabric workspace, create a new **Eventstream** named `EnergyReadingsStream`.
3. **Source:** Azure Event Hub
   - Namespace: `demo-fabric-agents-rti`
   - Event Hub: `energy-readings`
   - Consumer group: `energy-cg`
   - Authentication: Shared Access Policy (connection string from B3)
4. **Destination:** KQL Database
   - Workspace: your Fabric workspace
   - KQL Database: `ManufacturingKQL`
   - Table: `EnergyReadings`
5. Save and **Publish** the Eventstream. Confirm the status shows **Running**.

---

## Phase D — Notebooks

### D1 — Carbon Intensity notebook (run once)

Run the notebook `notebooks/00_carbon_intensity.py`. No configuration needed. Re-running safely overwrites data.

### D2 - Seed Dimensions notebook (run once)

Run the notebook `notebooks/01_seed_dimensions.py`.

> For the demo, use `TARIFF-DEMO` (flat €0.14/kWh) to keep cost calculations simple and explainable. In a real deployment you would join against peak/off-peak tariffs dynamically.

### D3 — Energy simulator notebook

This is the main simulator. Connect to SQL Server using your chosen authentication method and run the continuous loop.

Run the notebook `notebooks/02_energy_simulator.py`.
---

## Phase E — Insights Layer

### E1 — Power BI real-time dashboard

Create a new Power BI report connected to `ManufacturingKQL` via **Direct Query** (not import mode — you need live data).

Recommended tiles and their KQL queries:

**Cost per unit — per machine (last 10 minutes)**
```kql
EnergyReadings
| where Timestamp > ago(10m)
| summarize AvgCostPerUnit = avg(CostPerUnit) by MachineId
| order by AvgCostPerUnit desc
```

**Live energy draw — all machines**
```kql
EnergyReadings
| where Timestamp > ago(5m)
| summarize AvgPowerKw = avg(PowerKw) by MachineId, bin(Timestamp, 30s)
| order by Timestamp asc
```

**OEE trend — last 15 minutes**
```kql
EnergyReadings
| where Timestamp > ago(15m)
| summarize AvgOee = avg(OeePercent) by MachineId, bin(Timestamp, 1m)
```

**Carbon cost per unit — joined with carbon intensity**
```kql
let LatestCarbon = CarbonIntensity | top 1 by Timestamp desc;
EnergyReadings
| where Timestamp > ago(10m)
| summarize AvgPowerKw = avg(PowerKw), AvgUnits = avg(todouble(ProductionUnits))
  by MachineId
| extend CarbonCostPerUnit = (AvgPowerKw * (5.0/3600)) * toscalar(LatestCarbon | project GCo2PerKwh)
         / AvgUnits
```

**Cumulative shift cost ticker**
```kql
EnergyReadings
| where ShiftId == "Morning"   // parameterise by current shift
| summarize TotalCostEur = sum(CostThisTick)
```

Set the dashboard **auto-refresh to 5 seconds** in Power BI service page settings.

### E2 — Fabric data agent

1. In the Fabric workspace, create a new **Data Agent**.
2. Add data sources:
   - KQL Database: `ManufacturingKQL` (tables: `ces_energy_readings_curated`, `energy_by_shift_table`)
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

4. Suggested demo Q&A flows to rehearse:

| Question | What it demonstrates |
|---|---|
| "Which machine has the highest cost per unit right now?" | Basic real-time aggregation |
| "Why is WELD-L2-A so expensive compared to WELD-L2-B?" | Anomaly identification with context |
| "What is the carbon cost per unit on Line 3 right now?" | Cross-stream join (energy + carbon) |
| "Which machine has the worst OEE trend in the last 10 minutes?" | Time-series trend analysis |
| "What is the combined financial impact of both anomalies this shift?" | Compound multi-machine query |
| "If COAT-L3-A ran at 85% OEE instead of 61%, how much would we save per shift?" | Hypothetical / what-if |

---

## Demo Day Checklist

### 30 minutes before

- [ ] Start **D1** (carbon intensity notebook) — confirm rows appearing in `CarbonIntensity` KQL table
- [ ] Start **D2** (energy simulator notebook) — confirm rows appearing in `EnergyReadings` KQL table
- [ ] Open Power BI dashboard — confirm tiles refreshing with live data
- [ ] Open data agent — run one test question to confirm it responds correctly
- [ ] Check Eventstream status for both pipelines — both should show **Running**

### During the demo

- Simulator runs automatically through all 4 phases — no manual intervention needed
- Phase 2 (energy spike) begins at approximately **4 minutes** after D2 starts
- Phase 3 (OEE degradation) begins at approximately **7 minutes** after D2 starts
- Both anomalies are sustained from **11 minutes** onwards — this is when to ask the compound agent questions

### If something goes wrong

- **No data on dashboard:** Check Eventstream status. If stopped, restart both pipelines. Data resumes within 30 seconds.
- **Notebook disconnected:** Restart D2. The simulator picks up from the current iteration — cumulative kWh will reset but the demo phases will replay correctly.
- **Agent not responding:** Refresh the agent page. KQL queries run fresh on each question so there is no stale state to clear.

---

## Repository Structure

```
fabric-rti/
├── README.md                        ← this file
├── sql/
│   └── setup.sql                    ← Phase A scripts (DDL + CDC + auth)
├── notebooks/
│   ├── 00_seed_dimensions.ipynb        ← Phase B2: Lakehouse dimension tables
│   ├── 01_carbon_intensity.ipynb       ← Phase D1: carbon API → Event Hub
│   └── 02_energy_simulator.ipynb       ← Phase D2: simulator → SQL Server
└── kql/
    └── schema.kql                   ← Phase B4: all KQL table and view definitions
```
