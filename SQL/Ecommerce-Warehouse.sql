
USE ROLE SYSADMIN;
--Creating database
CREATE OR REPLACE DATABASE WAREHOUSE_DB1;
USE DATABASE WAREHOUSE_DB1;
--Following Medallion Architecture
CREATE OR REPLACE SCHEMA BRONZE;
CREATE OR REPLACE SCHEMA SILVER;
CREATE OR REPLACE SCHEMA GOLD;

--Raw tables Creation
CREATE OR REPLACE TABLE BRONZE.orders_raw (
  order_id INT,
  customer_id INT,
  order_date DATE,
  promised_ship_date DATE,
  order_value NUMBER(10,2),
  item_count INT,
  order_channel STRING,
  priority_flag INT,
  status STRING
);

CREATE OR REPLACE TABLE BRONZE.customers_raw (
  customer_id INT,
  customer_name STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  segment STRING,
  registration_date DATE
);

CREATE OR REPLACE TABLE BRONZE.employees_raw (
  employee_id INT,
  employee_name STRING,
  role STRING,
  shift_start TIMESTAMP,
  shift_end TIMESTAMP,
  hub_id STRING,
  join_date DATE,
  status STRING
);

CREATE OR REPLACE TABLE BRONZE.inventory_raw (
  inventory_id INT,
  sku_id STRING,
  sku_name STRING,
  category STRING,
  on_hand_qty INT,
  reserved_qty INT,
  reorder_level INT,
  last_restock_date DATE,
  warehouse_id STRING
);

CREATE OR REPLACE TABLE BRONZE.events_raw (
  event_id INT,
  order_id INT,
  employee_id INT,
  event_type STRING,
  event_time TIMESTAMP,
  remarks STRING
);

USE ROLE ACCOUNTADMIN;--Creating a Storage Integration requires ACCOUNTADMIN
--Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::695974226905:role/Yogita@snowflake'
STORAGE_ALLOWED_LOCATIONS = ('s3://amz-s3-69/');
-- connects Snowflake to S3 securely

DESC INTEGRATION s3_int;

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;

-- CREATING EXTERNAL STAGE -- pointing to S3 location
CREATE OR REPLACE STAGE ext_stage
URL = 's3://amz-s3-69/Warehouse_data/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = csv_format;

list @ext_stage;
-- loads data into raw tables
COPY INTO BRONZE.orders_raw FROM @ext_stage/orders_master.csv;
COPY INTO BRONZE.customers_raw FROM @ext_stage/customers_master.csv;
COPY INTO BRONZE.employees_raw FROM @ext_stage/employees_master.csv;
COPY INTO BRONZE.inventory_raw FROM @ext_stage/inventory_master.csv;
COPY INTO BRONZE.events_raw FROM @ext_stage/events_master.csv;

INSERT INTO BRONZE.orders_raw SELECT * FROM BRONZE.orders_raw;
INSERT INTO BRONZE.events_raw SELECT * FROM BRONZE.events_raw;
INSERT INTO BRONZE.customers_raw SELECT * FROM BRONZE.customers_raw;

SELECT * FROM BRONZE.orders_raw;
SELECT * FROM BRONZE.customers_raw;
SELECT * FROM BRONZE.events_raw;
SELECT * FROM BRONZE.employees_raw;
SELECT * FROM BRONZE.inventory_raw;


CREATE OR REPLACE TEMP TABLE temp_customers AS
SELECT DISTINCT * FROM BRONZE.customers_raw;

CREATE OR REPLACE TEMP TABLE temp_orders AS
SELECT DISTINCT * FROM BRONZE.orders_raw;

CREATE OR REPLACE TEMP TABLE temp_employees AS
SELECT DISTINCT * FROM BRONZE.employees_raw;

--SILVER LAYER-VALIDATION
CREATE OR REPLACE TABLE SILVER.orders_clean AS
SELECT DISTINCT o.*
FROM temp_orders o
JOIN temp_customers c
ON o.customer_id = c.customer_id
WHERE o.order_id IS NOT NULL
AND o.order_value >= 0
AND o.item_count >= 1
AND o.order_date <= o.promised_ship_date;

CREATE OR REPLACE TABLE SILVER.customers_clean AS
SELECT DISTINCT *
FROM BRONZE.customers_raw
WHERE customer_id IS NOT NULL;

CREATE OR REPLACE TABLE SILVER.employees_clean AS
SELECT DISTINCT *
FROM BRONZE.employees_raw
WHERE employee_id IS NOT NULL
AND shift_start<= shift_end;

CREATE OR REPLACE TABLE SILVER.inventory_clean AS
SELECT DISTINCT *
FROM BRONZE.inventory_raw
WHERE reserved_qty <= on_hand_qty;

CREATE OR REPLACE TABLE SILVER.events_clean AS
SELECT DISTINCT e.*
FROM (
    SELECT DISTINCT * FROM BRONZE.events_raw
) e
JOIN temp_orders o
ON e.order_id = o.order_id
JOIN temp_employees emp
ON e.employee_id = emp.employee_id
WHERE e.event_type IN ('pick','pack','qc','ship')
AND e.event_time IS NOT NULL;

SELECT * FROM SILVER.orders_clean;
SELECT * FROM SILVER.customers_clean;
SELECT * FROM SILVER.events_clean;
SELECT * FROM SILVER.inventory_clean;
SELECT * FROM SILVER.employees_clean;

--Using Streams to capture changes on the tables
CREATE OR REPLACE STREAM orders_stream ON TABLE BRONZE.orders_raw;
CREATE OR REPLACE STREAM customers_stream ON TABLE BRONZE.customers_raw;
CREATE OR REPLACE STREAM employees_stream ON TABLE BRONZE.employees_raw;
CREATE OR REPLACE STREAM events_stream ON TABLE BRONZE.events_raw;
CREATE OR REPLACE STREAM inventory_stream ON TABLE BRONZE.inventory_raw;

--Adding incremental data
COPY INTO BRONZE.orders_raw
FROM @ext_stage/orders_inc.csv;

COPY INTO BRONZE.customers_raw
FROM @ext_stage/customers_inc.csv;

COPY INTO BRONZE.events_raw
FROM @ext_stage/events_inc.csv;

COPY INTO BRONZE.employees_raw
FROM @ext_stage/employees_inc.csv;

COPY INTO BRONZE.inventory_raw
FROM @ext_stage/inventory_inc.csv;

SELECT * FROM BRONZE.CUSTOMERS_RAW;
SELECT * FROM BRONZE.INVENTORY_RAW;
SELECT * FROM BRONZE.ORDERS_RAW;
SELECT * FROM BRONZE.EVENTS_RAW;
SELECT * FROM BRONZE.EMPLOYEES_RAW;

--Streams capture changes
SELECT * FROM EMPLOYEES_STREAM;
SELECT * FROM CUSTOMERS_STREAM;
SELECT * FROM EVENTS_STREAM;
SELECT * FROM ORDERS_STREAM;
SELECT * FROM INVENTORY_STREAM;

--PROCEDURE (BRONZE → SILVER)
CREATE OR REPLACE PROCEDURE sp_bronze_to_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- ORDERS
MERGE INTO SILVER.orders_clean t
USING (
  SELECT s.*
  FROM orders_stream s
  JOIN SILVER.customers_clean c
  ON s.customer_id = c.customer_id
  WHERE s.order_value >= 0
  AND s.item_count >= 1
) s
ON t.order_id = s.order_id

WHEN MATCHED THEN UPDATE SET
  t.customer_id = s.customer_id,
  t.order_value = s.order_value,
  t.status = s.status

WHEN NOT MATCHED THEN INSERT VALUES (
  s.order_id, s.customer_id, s.order_date,
  s.promised_ship_date, s.order_value,
  s.item_count, s.order_channel,
  s.priority_flag, s.status
);

-- EVENTS
MERGE INTO SILVER.events_clean t
USING (
  SELECT e.*
  FROM events_stream e
  JOIN SILVER.orders_clean o
  ON e.order_id = o.order_id
  JOIN SILVER.employees_clean emp
  ON e.employee_id = emp.employee_id
  WHERE e.event_type IN ('pick','pack','qc','ship')
) s
ON t.event_id = s.event_id

WHEN MATCHED THEN UPDATE SET
  t.event_type = s.event_type,
  t.event_time = s.event_time

WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.order_id, s.employee_id,
  s.event_type, s.event_time, s.remarks
);

RETURN 'BRONZE TO SILVER DONE';

END;
$$;

CREATE OR REPLACE TASK task_bronze_to_silver
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
CALL sp_bronze_to_silver();

ALTER TASK task_bronze_to_silver RESUME;


--GOLD LAYER

--DIM_CUSTOMERS (SCD TYPE 2)
-- Customer Dimension with history tracking (SCD Type 2)
CREATE OR REPLACE TABLE GOLD.dim_customers (
  customer_sk INT AUTOINCREMENT, -- surrogate key
  customer_id INT, -- business key
  customer_name STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  segment STRING,
  registration_date DATE,

  effective_date DATE, -- record start
  end_date DATE,       -- record end
  is_active BOOLEAN    -- current record flag
);

--DIM_EMPLOYEES (SCD TYPE 2)
-- Employee Dimension with history
CREATE OR REPLACE TABLE GOLD.dim_employees (
  employee_sk INT AUTOINCREMENT,
  employee_id INT,
  employee_name STRING,
  role STRING,
  shift_start TIMESTAMP,
  shift_end TIMESTAMP,
  hub_id STRING,
  join_date DATE,
  status STRING,

  effective_date DATE,
  end_date DATE,
  is_active BOOLEAN
);

--DIM_INVENTORY (SNAPSHOT TABLE)
---- Inventory dimension (current snapshot)
CREATE OR REPLACE TABLE GOLD.dim_inventory AS
SELECT * FROM SILVER.inventory_clean;

----FACT TABLES
-- Fact table for orders
CREATE OR REPLACE TABLE GOLD.fact_orders AS
SELECT * FROM SILVER.orders_clean;

---- Fact table capturing warehouse operations
CREATE OR REPLACE TABLE GOLD.fact_events AS
SELECT * FROM SILVER.events_clean;


--INITIAL LOAD INTO DIMENSIONS

-- Load customers initially
INSERT INTO GOLD.dim_customers (
  customer_id,customer_name,email,phone,city,state,
  segment,registration_date,effective_date,end_date,is_active
)
SELECT 
  customer_id,
  customer_name,
  email,
  phone,
  city,
  state,
  segment,
  registration_date,
  CURRENT_DATE,
  NULL,
  TRUE
FROM SILVER.customers_clean;

-- Load employees initially
INSERT INTO GOLD.dim_employees (
  employee_id,
  employee_name,
  role,
  shift_start,
  shift_end,
  hub_id,
  join_date,
  status,
  effective_date,
  end_date,
  is_active
)
SELECT 
  employee_id,
  employee_name,
  role,
  shift_start,
  shift_end,
  hub_id,
  join_date,
  status,
  CURRENT_DATE,
  NULL,
  TRUE
FROM SILVER.employees_clean;

--SCD TYPE 2 PROCEDURES
--CUSTOMERS
CREATE OR REPLACE PROCEDURE scd2_customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- Step 1: expire old records if changed
UPDATE GOLD.dim_customers t
SET is_active = FALSE,
    end_date = CURRENT_DATE
FROM SILVER.customers_clean s
WHERE t.customer_id = s.customer_id
AND t.is_active = TRUE
AND (
    NVL(t.city,'') <> NVL(s.city,'')
    OR NVL(t.segment,'') <> NVL(s.segment,'')
);

-- Step 2: insert new version
INSERT INTO GOLD.dim_customers
SELECT 
  s.customer_id,
  s.customer_name,
  s.email,
  s.phone,
  s.city,
  s.state,
  s.segment,
  s.registration_date,
  CURRENT_DATE,
  NULL,
  TRUE
FROM SILVER.customers_clean s
LEFT JOIN GOLD.dim_customers t
ON s.customer_id = t.customer_id AND t.is_active = TRUE
WHERE t.customer_id IS NULL
OR (
    NVL(t.city,'') <> NVL(s.city,'')
    OR NVL(t.segment,'') <> NVL(s.segment,'')
);

RETURN 'CUSTOMERS SCD2 DONE';
END;
$$;

--EMPLOYEES
CREATE OR REPLACE PROCEDURE scd2_employees()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

UPDATE GOLD.dim_employees t
SET is_active = FALSE,
    end_date = CURRENT_DATE
FROM SILVER.employees_clean s
WHERE t.employee_id = s.employee_id
AND t.is_active = TRUE
AND (
    NVL(t.role,'') <> NVL(s.role,'')
);

INSERT INTO GOLD.dim_employees
SELECT 
  s.employee_id,
  s.employee_name,
  s.role,
  s.shift_start,
  s.shift_end,
  s.hub_id,
  s.join_date,
  s.status,
  CURRENT_DATE,
  NULL,
  TRUE
FROM SILVER.employees_clean s
LEFT JOIN GOLD.dim_employees t
ON s.employee_id = t.employee_id AND t.is_active = TRUE
WHERE t.employee_id IS NULL
OR NVL(t.role,'') <> NVL(s.role,'');

RETURN 'EMPLOYEES SCD2 DONE';
END;
$$;

--MASTER SCD2
CREATE OR REPLACE PROCEDURE run_scd2()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
CALL scd2_customers();
CALL scd2_employees();
RETURN 'SCD2 COMPLETED';
END;
$$;

ALTER TASK WAREHOUSE_DB1.GOLD.TASK_BRONZE_TO_SILVER SUSPEND;

--SCD2 TASK
CREATE OR REPLACE TASK task_scd2
WAREHOUSE = COMPUTE_WH
AFTER task_bronze_to_silver
AS
CALL run_scd2();

ALTER TASK WAREHOUSE_DB1.GOLD.TASK_BRONZE_TO_SILVER RESUME;
ALTER TASK WAREHOUSE_DB1.GOLD.TASK_BRONZE_TO_SILVER SUSPEND;
ALTER TASK task_scd2 RESUME;

-- =====================================================
-- ORDER LIFECYCLE VIEW
-- Combines events to track full order journey
-- =====================================================

CREATE OR REPLACE VIEW GOLD.order_lifecycle AS
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.promised_ship_date,

    -- Capture each stage time
    MIN(CASE WHEN e.event_type='pick' THEN e.event_time END) AS pick_time,
    MIN(CASE WHEN e.event_type='pack' THEN e.event_time END) AS pack_time,
    MIN(CASE WHEN e.event_type='qc' THEN e.event_time END) AS qc_time,
    MIN(CASE WHEN e.event_type='ship' THEN e.event_time END) AS ship_time

FROM GOLD.fact_orders o
LEFT JOIN GOLD.fact_events e
ON o.order_id = e.order_id
GROUP BY o.order_id, o.customer_id, o.order_date, o.promised_ship_date;

SELECT * FROM GOLD.order_lifecycle;

-- =====================================================
-- ANOMALY TABLE
-- Stores all detected issues
-- =====================================================

CREATE OR REPLACE TABLE GOLD.anomalies (
    id STRING,
    issue_type STRING,
    description STRING,
    detected_at TIMESTAMP
);

--ANOMALY DETECTION PROCEDURES
--1.SLA BREACH
-- Orders shipped after promised date
CREATE OR REPLACE PROCEDURE detect_sla_breach()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO GOLD.anomalies
SELECT 
    order_id,
    'SLA_BREACH',
    'Order shipped late',
    CURRENT_TIMESTAMP
FROM GOLD.order_lifecycle
WHERE ship_time > promised_ship_date;

RETURN 'SLA BREACH DONE';

END;
$$;

--2.MIS-PICK
-- QC failed indicates mis-pick
CREATE OR REPLACE PROCEDURE detect_mispick()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO GOLD.anomalies
SELECT 
    order_id,
    'MIS_PICK',
    'QC Failed',
    CURRENT_TIMESTAMP
FROM GOLD.fact_events
WHERE event_type = 'qc'
AND LOWER(remarks) LIKE '%fail%';

RETURN 'MIS-PICK DONE';

END;
$$;

--PRODUCTIVITY ISSUE
-- Detect employees with abnormal workload
CREATE OR REPLACE PROCEDURE detect_productivity_issue()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO GOLD.anomalies
SELECT 
    employee_id,
    'PRODUCTIVITY',
    'Abnormal event count',
    CURRENT_TIMESTAMP
FROM (
    SELECT 
        employee_id,
        COUNT(*) AS event_count,
        AVG(COUNT(*)) OVER() AS avg_count
    FROM GOLD.fact_events
    GROUP BY employee_id
)
WHERE event_count < avg_count * 0.5
   OR event_count > avg_count * 2;

RETURN 'PRODUCTIVITY DONE';

END;
$$;

--INVENTORY ISSUE
-- Reserved > Available
CREATE OR REPLACE PROCEDURE detect_inventory_issue()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO GOLD.anomalies
SELECT 
    inventory_id,
    'INVENTORY',
    'Reserved exceeds stock',
    CURRENT_TIMESTAMP
FROM GOLD.dim_inventory
WHERE reserved_qty > on_hand_qty;

RETURN 'INVENTORY DONE';

END;
$$;


--EVENT SEQUENCE ERROR
-- Wrong sequence: pick → pack → qc → ship
CREATE OR REPLACE PROCEDURE detect_sequence_error()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO GOLD.anomalies
SELECT 
    order_id,
    'SEQUENCE_ERROR',
    'Incorrect event order',
    CURRENT_TIMESTAMP
FROM (
    SELECT 
        order_id,
        LISTAGG(event_type, '->') 
        WITHIN GROUP (ORDER BY event_time) AS seq
    FROM GOLD.fact_events
    GROUP BY order_id
)
WHERE seq NOT LIKE 'pick%pack%qc%ship%'
OR seq NOT LIKE '%pick%'
OR seq NOT LIKE '%pack%'
OR seq NOT LIKE '%qc%'
OR seq NOT LIKE '%ship%';

RETURN 'SEQUENCE DONE';

END;
$$;

--MASTER ANOMALY PROCEDURE

-- Runs all anomaly checks
CREATE OR REPLACE PROCEDURE run_all_anomalies()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
TRUNCATE TABLE GOLD.anomalies;

CALL detect_sla_breach();
CALL detect_mispick();
CALL detect_productivity_issue();
CALL detect_inventory_issue();
CALL detect_sequence_error();

RETURN 'ALL ANOMALIES DONE';

END;
$$;

CALL run_all_anomalies();
--ANOMALY TASK
CREATE OR REPLACE TASK task_anomalies
WAREHOUSE = COMPUTE_WH
AFTER task_scd2
AS
CALL run_all_anomalies();
SELECT * FROM GOLD.anomalies;
ALTER TASK task_anomalies RESUME;

--KPI VIEWS
--ORDER PROCESSING SLA %
CREATE OR REPLACE VIEW GOLD.v_kpi_sla AS
SELECT ROUND(
    COUNT(CASE WHEN ship_time <= promised_ship_date THEN 1 END)*100.0
    / COUNT(*), 2
) AS sla_score
FROM GOLD.order_lifecycle;

--WAREHOUSE THROUGHPUT RATE
CREATE OR REPLACE VIEW GOLD.v_kpi_throughput AS
SELECT 
    DATE_TRUNC('hour', event_time) AS hour,
    COUNT(*) AS events_per_hour
FROM GOLD.fact_events
GROUP BY hour
ORDER BY hour;

--EMPLOYEE PRODUCTIVITY INDEX
CREATE OR REPLACE VIEW GOLD.v_kpi_productivity AS
SELECT 
e.employee_id,
d.role,
COUNT(*) AS total_events,
DATEDIFF('hour', d.shift_start, d.shift_end) AS shift_hours,
ROUND(COUNT(*) / NULLIF(DATEDIFF('hour', d.shift_start, d.shift_end),0),2) AS productivity_index
FROM GOLD.fact_events e
JOIN GOLD.dim_employees d
ON e.employee_id = d.employee_id
WHERE d.is_active = TRUE
GROUP BY e.employee_id, d.role, d.shift_start, d.shift_end;

--INVENTORY AVAILABILITY SCORE
CREATE OR REPLACE VIEW GOLD.v_kpi_inventory AS
SELECT ROUND(
    COUNT(CASE WHEN on_hand_qty >= reserved_qty THEN 1 END)*100.0
    / COUNT(*), 2
) AS inventory_score
FROM GOLD.dim_inventory;

--ORDER ANOMALY RATE
CREATE OR REPLACE VIEW GOLD.v_kpi_anomaly AS
SELECT ROUND(
    COUNT(DISTINCT id)*100.0 /
    NULLIF((SELECT COUNT(*) FROM GOLD.fact_orders),0), 2
) AS anomaly_rate
FROM GOLD.anomalies;

--DATA GOVERNANCE
--MASKING (EMAIL)
CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING)
RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() = 'SYSADMIN' THEN val
    ELSE '****@masked.com'
END;

ALTER TABLE GOLD.dim_customers
MODIFY COLUMN email SET MASKING POLICY mask_email;


--MASKING (PHONE)
CREATE OR REPLACE MASKING POLICY mask_phone AS (val STRING)
RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() = 'SYSADMIN' THEN val
    ELSE 'XXXXXX'
END;

ALTER TABLE GOLD.dim_customers
MODIFY COLUMN phone SET MASKING POLICY mask_phone;

--ROW LEVEL SECURITY
CREATE OR REPLACE ROW ACCESS POLICY rls_customer
AS (state STRING)
RETURNS BOOLEAN ->
CURRENT_ROLE() = 'SYSADMIN'
OR state = 'AP';

ALTER TABLE GOLD.dim_customers
ADD ROW ACCESS POLICY rls_customer ON (state);

SELECT * FROM GOLD.dim_customers;




