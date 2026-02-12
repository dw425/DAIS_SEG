-- Healthcare Analytics Data Warehouse
-- Sample input for SEG Demo — Oracle-style DDL with complex constraints

CREATE TABLE patients (
    patient_id          NUMBER(12) PRIMARY KEY,
    mrn                 VARCHAR2(20) NOT NULL UNIQUE,
    first_name          VARCHAR2(100) NOT NULL,
    last_name           VARCHAR2(100) NOT NULL,
    date_of_birth       DATE NOT NULL,
    gender              CHAR(1) CHECK (gender IN ('M', 'F', 'O', 'U')),
    blood_type          VARCHAR2(5),
    ssn_hash            RAW(32),
    primary_phone       VARCHAR2(20),
    email               VARCHAR2(200),
    insurance_provider  VARCHAR2(100),
    insurance_policy_no VARCHAR2(50),
    emergency_contact   VARCHAR2(200),
    admission_count     NUMBER(5) DEFAULT 0,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_visit_date     DATE
);

CREATE TABLE providers (
    provider_id     NUMBER(10) PRIMARY KEY,
    npi             VARCHAR2(10) NOT NULL UNIQUE,
    first_name      VARCHAR2(100) NOT NULL,
    last_name       VARCHAR2(100) NOT NULL,
    specialty       VARCHAR2(100),
    department      VARCHAR2(100),
    license_state   CHAR(2),
    is_active       NUMBER(1) DEFAULT 1
);

CREATE TABLE encounters (
    encounter_id        NUMBER(15) PRIMARY KEY,
    patient_id          NUMBER(12) NOT NULL REFERENCES patients(patient_id),
    provider_id         NUMBER(10) NOT NULL REFERENCES providers(provider_id),
    encounter_type      VARCHAR2(30) CHECK (encounter_type IN ('inpatient', 'outpatient', 'emergency', 'telehealth', 'observation')),
    admit_date          TIMESTAMP NOT NULL,
    discharge_date      TIMESTAMP,
    status              VARCHAR2(20) DEFAULT 'active',
    primary_diagnosis   VARCHAR2(10),
    drg_code            VARCHAR2(10),
    location            VARCHAR2(100),
    bed_id              VARCHAR2(20),
    total_charges       NUMBER(12,2),
    insurance_paid      NUMBER(12,2),
    patient_paid        NUMBER(12,2)
);

CREATE TABLE diagnoses (
    diagnosis_id    NUMBER(15) PRIMARY KEY,
    encounter_id    NUMBER(15) NOT NULL REFERENCES encounters(encounter_id),
    icd10_code      VARCHAR2(10) NOT NULL,
    description     VARCHAR2(500),
    is_primary      NUMBER(1) DEFAULT 0,
    diagnosed_by    NUMBER(10) REFERENCES providers(provider_id),
    diagnosed_date  TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE lab_results (
    result_id       NUMBER(15) PRIMARY KEY,
    encounter_id    NUMBER(15) NOT NULL REFERENCES encounters(encounter_id),
    patient_id      NUMBER(12) NOT NULL REFERENCES patients(patient_id),
    test_code       VARCHAR2(20) NOT NULL,
    test_name       VARCHAR2(200) NOT NULL,
    result_value    VARCHAR2(100),
    result_numeric  NUMBER(12,4),
    unit            VARCHAR2(30),
    reference_low   NUMBER(12,4),
    reference_high  NUMBER(12,4),
    abnormal_flag   CHAR(1) CHECK (abnormal_flag IN ('H', 'L', 'N', 'C')),
    status          VARCHAR2(20) DEFAULT 'final',
    collected_date  TIMESTAMP NOT NULL,
    reported_date   TIMESTAMP
);

CREATE TABLE medications (
    medication_id   NUMBER(15) PRIMARY KEY,
    encounter_id    NUMBER(15) NOT NULL REFERENCES encounters(encounter_id),
    patient_id      NUMBER(12) NOT NULL REFERENCES patients(patient_id),
    ndc_code        VARCHAR2(12),
    drug_name       VARCHAR2(300) NOT NULL,
    dosage          VARCHAR2(100),
    route           VARCHAR2(50),
    frequency       VARCHAR2(50),
    prescriber_id   NUMBER(10) REFERENCES providers(provider_id),
    start_date      DATE NOT NULL,
    end_date        DATE,
    status          VARCHAR2(20) CHECK (status IN ('active', 'completed', 'discontinued', 'cancelled'))
);

CREATE TABLE vital_signs (
    vital_id        NUMBER(15) PRIMARY KEY,
    encounter_id    NUMBER(15) NOT NULL REFERENCES encounters(encounter_id),
    patient_id      NUMBER(12) NOT NULL REFERENCES patients(patient_id),
    measurement_time TIMESTAMP NOT NULL,
    temperature     NUMBER(5,1),
    heart_rate      NUMBER(4),
    systolic_bp     NUMBER(4),
    diastolic_bp    NUMBER(4),
    respiratory_rate NUMBER(3),
    spo2            NUMBER(5,2),
    weight_kg       NUMBER(6,2),
    height_cm       NUMBER(5,1),
    pain_scale      NUMBER(2) CHECK (pain_scale BETWEEN 0 AND 10),
    recorded_by     NUMBER(10) REFERENCES providers(provider_id)
);
