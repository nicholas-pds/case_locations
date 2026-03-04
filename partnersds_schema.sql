-- ============================================================
-- PartnersDS Lab Management — PostgreSQL Schema
-- ============================================================

-- updated_at auto-trigger function
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- LOOKUP: case_statuses
-- ============================================================
CREATE TABLE IF NOT EXISTS case_statuses (
    status_id   SERIAL PRIMARY KEY,
    status_name VARCHAR(100) NOT NULL UNIQUE,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO case_statuses (status_name) VALUES
    ('In Production'),
    ('Invoiced'),
    ('On Hold'),
    ('Cancelled'),
    ('Shipped'),
    ('Received'),
    ('Pending'),
    ('Remake Full Charge'),
    ('Remake No Charge')
ON CONFLICT (status_name) DO NOTHING;

-- ============================================================
-- LOOKUP: departments
-- ============================================================
CREATE TABLE IF NOT EXISTS departments (
    department_id   SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    department_code VARCHAR(20),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO departments (department_name, department_code) VALUES
    ('Crown & Bridge',  'CB'),
    ('Partial',         'PT'),
    ('Denture',         'DN'),
    ('Implant',         'IM'),
    ('Orthodontics',    'OT'),
    ('Finishing',       'FN'),
    ('Wax',             'WX'),
    ('Metal',           'MT'),
    ('Shipping',        'SH'),
    ('Admin',           'AD')
ON CONFLICT (department_name) DO NOTHING;

-- ============================================================
-- EMPLOYEES
-- ============================================================
CREATE TABLE IF NOT EXISTS employees (
    employee_id   SERIAL PRIMARY KEY,
    employee_code VARCHAR(50) UNIQUE,       -- external system ID
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    department_id INT REFERENCES departments(department_id),
    role          VARCHAR(100),             -- Technician, Sales Rep (AM), Manager, etc.
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    hire_date     DATE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_employees_employee_code ON employees(employee_code);
CREATE INDEX IF NOT EXISTS idx_employees_department_id ON employees(department_id);

CREATE OR REPLACE TRIGGER trg_employees_updated_at
    BEFORE UPDATE ON employees
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- CUSTOMERS (Practices)
-- ============================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id   SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) UNIQUE,       -- external system ID
    practice_name VARCHAR(200) NOT NULL,
    salesperson_id INT REFERENCES employees(employee_id),
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city          VARCHAR(100),
    state         CHAR(2),
    zip           VARCHAR(10),
    phone         VARCHAR(20),
    email         VARCHAR(200),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_customer_code  ON customers(customer_code);
CREATE INDEX IF NOT EXISTS idx_customers_salesperson_id ON customers(salesperson_id);

CREATE OR REPLACE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- PRACTICE DOCTORS
-- ============================================================
CREATE TABLE IF NOT EXISTS practice_doctors_master (
    doctor_id      SERIAL PRIMARY KEY,
    first_name     VARCHAR(100),
    last_name      VARCHAR(100) NOT NULL,
    license_number VARCHAR(50),
    specialty      VARCHAR(100),
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE TRIGGER trg_practice_doctors_master_updated_at
    BEFORE UPDATE ON practice_doctors_master
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS practice_doctors_link (
    link_id     SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    doctor_id   INT NOT NULL REFERENCES practice_doctors_master(doctor_id) ON DELETE CASCADE,
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE,
    linked_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, doctor_id)
);

CREATE INDEX IF NOT EXISTS idx_practice_doctors_link_customer ON practice_doctors_link(customer_id);
CREATE INDEX IF NOT EXISTS idx_practice_doctors_link_doctor   ON practice_doctors_link(doctor_id);

-- ============================================================
-- CASES
-- ============================================================
CREATE TABLE IF NOT EXISTS cases (
    case_id        SERIAL PRIMARY KEY,
    case_number    VARCHAR(50) NOT NULL UNIQUE,   -- external system case number
    og_case_number VARCHAR(50),                   -- original case # if this is a remake
    is_remake      BOOLEAN NOT NULL DEFAULT FALSE,
    remake_reason  TEXT,
    customer_id    INT REFERENCES customers(customer_id),
    doctor_id      INT REFERENCES practice_doctors_master(doctor_id),
    salesperson_id INT REFERENCES employees(employee_id),
    status_id      INT REFERENCES case_statuses(status_id),
    department_id  INT REFERENCES departments(department_id),
    product_type   VARCHAR(200),
    top_product    VARCHAR(200),
    pan_number     VARCHAR(50),
    due_date       DATE,
    received_date  DATE,
    invoice_date   DATE,
    ship_date      DATE,
    invoice_amount NUMERIC(12,2),
    notes          TEXT,
    remake_notes   TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cases_case_number    ON cases(case_number);
CREATE INDEX IF NOT EXISTS idx_cases_og_case_number ON cases(og_case_number);
CREATE INDEX IF NOT EXISTS idx_cases_customer_id    ON cases(customer_id);
CREATE INDEX IF NOT EXISTS idx_cases_salesperson_id ON cases(salesperson_id);
CREATE INDEX IF NOT EXISTS idx_cases_status_id      ON cases(status_id);
CREATE INDEX IF NOT EXISTS idx_cases_invoice_date   ON cases(invoice_date);
CREATE INDEX IF NOT EXISTS idx_cases_is_remake      ON cases(is_remake);
CREATE INDEX IF NOT EXISTS idx_cases_department_id  ON cases(department_id);

CREATE OR REPLACE TRIGGER trg_cases_updated_at
    BEFORE UPDATE ON cases
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- CASE TASKS (current open tasks)
-- ============================================================
CREATE TABLE IF NOT EXISTS case_tasks (
    task_id      SERIAL PRIMARY KEY,
    case_id      INT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
    department_id INT REFERENCES departments(department_id),
    assigned_to  INT REFERENCES employees(employee_id),
    task_name    VARCHAR(200) NOT NULL,
    description  TEXT,
    status       VARCHAR(50) NOT NULL DEFAULT 'Pending', -- Pending, In Progress, Completed, On Hold
    priority     SMALLINT NOT NULL DEFAULT 5,
    due_date     DATE,
    completed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_case_tasks_case_id     ON case_tasks(case_id);
CREATE INDEX IF NOT EXISTS idx_case_tasks_assigned_to ON case_tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_case_tasks_status      ON case_tasks(status);

CREATE OR REPLACE TRIGGER trg_case_tasks_updated_at
    BEFORE UPDATE ON case_tasks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- CASE TASKS HISTORY (audit trail of task activity)
-- ============================================================
CREATE TABLE IF NOT EXISTS case_tasks_history (
    history_id    SERIAL PRIMARY KEY,
    case_id       INT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
    task_id       INT REFERENCES case_tasks(task_id) ON DELETE SET NULL,
    department_id INT REFERENCES departments(department_id),
    employee_id   INT REFERENCES employees(employee_id),
    task_name     VARCHAR(200),
    action        VARCHAR(100) NOT NULL, -- Started, Completed, Reassigned, On Hold, Cancelled
    old_status    VARCHAR(50),
    new_status    VARCHAR(50),
    notes         TEXT,
    action_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_case_tasks_history_case_id     ON case_tasks_history(case_id);
CREATE INDEX IF NOT EXISTS idx_case_tasks_history_employee_id ON case_tasks_history(employee_id);
CREATE INDEX IF NOT EXISTS idx_case_tasks_history_action_at   ON case_tasks_history(action_at);
CREATE INDEX IF NOT EXISTS idx_case_tasks_history_task_id     ON case_tasks_history(task_id);

-- ============================================================
-- SYNC LOG (SQL Server → PostgreSQL sync tracking)
-- ============================================================
CREATE TABLE IF NOT EXISTS sync_log (
    sync_id           SERIAL PRIMARY KEY,
    sync_type         VARCHAR(100) NOT NULL,                        -- cases_full, employees, incremental, etc.
    source_system     VARCHAR(100) NOT NULL DEFAULT 'SQL Server',
    target_system     VARCHAR(100) NOT NULL DEFAULT 'PostgreSQL',
    status            VARCHAR(50)  NOT NULL,                        -- running, success, failed, partial
    records_attempted INT,
    records_succeeded INT,
    records_failed    INT,
    error_message     TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sync_log_sync_type  ON sync_log(sync_type);
CREATE INDEX IF NOT EXISTS idx_sync_log_started_at ON sync_log(started_at);
CREATE INDEX IF NOT EXISTS idx_sync_log_status     ON sync_log(status);
