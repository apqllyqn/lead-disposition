-- =============================================================================
-- Lead Disposition System - Schema v1.0
-- Standalone database for contact lifecycle, cooldown, and TAM tracking
-- =============================================================================

-- =============================================================================
-- EXTENSIONS
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- ENUM TYPES
-- =============================================================================

CREATE TYPE disposition_status AS ENUM (
    'fresh',
    'in_sequence',
    'completed_no_response',
    'replied_positive',
    'replied_neutral',
    'replied_negative',
    'replied_hard_no',
    'bounced',
    'unsubscribed',
    'retouch_eligible',
    'stale_data',
    'job_change_detected',
    'won_customer',
    'lost_closed'
);

CREATE TYPE company_status AS ENUM (
    'fresh',
    'active',
    'cooling',
    'suppressed',
    'customer'
);

CREATE TYPE channel_type AS ENUM (
    'email',
    'linkedin',
    'phone'
);

CREATE TYPE ownership_change_reason AS ENUM (
    'first_claim',
    'expired',
    'manual_release',
    'admin_transfer'
);

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMPANIES
-- =============================================================================

CREATE TABLE companies (
    domain              TEXT PRIMARY KEY,
    name                TEXT,

    -- Aggregate state
    company_status      company_status NOT NULL DEFAULT 'fresh',
    company_suppressed  BOOLEAN NOT NULL DEFAULT false,
    suppressed_reason   TEXT,
    suppressed_at       TIMESTAMPTZ,

    -- Contact counters
    contacts_total      INTEGER NOT NULL DEFAULT 0,
    contacts_in_sequence INTEGER NOT NULL DEFAULT 0,
    contacts_touched    INTEGER NOT NULL DEFAULT 0,

    -- Timing
    last_contact_date   TIMESTAMPTZ,
    company_cooldown_until TIMESTAMPTZ,

    -- Customer tracking
    is_customer         BOOLEAN NOT NULL DEFAULT false,
    customer_since      TIMESTAMPTZ,

    -- Client ownership (first-mover deconfliction)
    client_owner_id     TEXT,
    client_owned_at     TIMESTAMPTZ,
    ownership_expires_at TIMESTAMPTZ,

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_companies_status ON companies(company_status);
CREATE INDEX idx_companies_suppressed ON companies(company_suppressed) WHERE company_suppressed = true;
CREATE INDEX idx_companies_customer ON companies(is_customer) WHERE is_customer = true;
CREATE INDEX idx_companies_owner ON companies(client_owner_id) WHERE client_owner_id IS NOT NULL;
CREATE INDEX idx_companies_ownership_expiry ON companies(ownership_expires_at)
    WHERE ownership_expires_at IS NOT NULL;

CREATE TRIGGER trg_companies_timestamp
    BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- =============================================================================
-- CONTACTS
-- =============================================================================

CREATE TABLE contacts (
    email               TEXT NOT NULL,
    client_id           TEXT NOT NULL,
    company_domain      TEXT NOT NULL REFERENCES companies(domain) ON DELETE CASCADE,

    -- Identity
    first_name          TEXT,
    last_name           TEXT,
    last_known_title    TEXT,
    last_known_company  TEXT,

    -- Disposition state
    disposition_status  disposition_status NOT NULL DEFAULT 'fresh',
    disposition_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Channel-specific last contact timestamps
    email_last_contacted    TIMESTAMPTZ,
    linkedin_last_contacted TIMESTAMPTZ,
    phone_last_contacted    TIMESTAMPTZ,

    -- Channel-specific cooldowns
    email_cooldown_until    TIMESTAMPTZ,
    linkedin_cooldown_until TIMESTAMPTZ,
    phone_cooldown_until    TIMESTAMPTZ,

    -- Channel-specific suppression
    email_suppressed        BOOLEAN NOT NULL DEFAULT false,
    linkedin_suppressed     BOOLEAN NOT NULL DEFAULT false,
    phone_suppressed        BOOLEAN NOT NULL DEFAULT false,

    -- Data freshness
    data_enriched_at    TIMESTAMPTZ,

    -- Outreach history
    sequence_count      INTEGER NOT NULL DEFAULT 0,

    -- Source tracking
    source_system       TEXT,
    source_id           TEXT,

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (email, client_id)
);

CREATE INDEX idx_contacts_domain ON contacts(company_domain);
CREATE INDEX idx_contacts_status ON contacts(disposition_status);
CREATE INDEX idx_contacts_client ON contacts(client_id);
CREATE INDEX idx_contacts_email_suppressed ON contacts(email_suppressed) WHERE email_suppressed = true;
CREATE INDEX idx_contacts_email_cooldown ON contacts(email_cooldown_until)
    WHERE email_cooldown_until IS NOT NULL;
CREATE INDEX idx_contacts_linkedin_cooldown ON contacts(linkedin_cooldown_until)
    WHERE linkedin_cooldown_until IS NOT NULL;
CREATE INDEX idx_contacts_enriched ON contacts(data_enriched_at);
CREATE INDEX idx_contacts_available ON contacts(disposition_status, email_suppressed, email_cooldown_until)
    WHERE disposition_status IN ('fresh', 'retouch_eligible') AND email_suppressed = false;

CREATE TRIGGER trg_contacts_timestamp
    BEFORE UPDATE ON contacts
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- =============================================================================
-- DISPOSITION HISTORY (append-only audit log)
-- =============================================================================

CREATE TABLE disposition_history (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_email       TEXT NOT NULL,
    contact_client_id   TEXT NOT NULL,
    previous_status     disposition_status,
    new_status          disposition_status NOT NULL,
    transition_reason   TEXT,
    triggered_by        TEXT NOT NULL DEFAULT 'system',
    campaign_id         TEXT,
    metadata            JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_history_contact ON disposition_history(contact_email, contact_client_id);
CREATE INDEX idx_history_created ON disposition_history(created_at DESC);
CREATE INDEX idx_history_campaign ON disposition_history(campaign_id) WHERE campaign_id IS NOT NULL;

-- =============================================================================
-- CLIENT OWNERSHIP HISTORY
-- =============================================================================

CREATE TABLE client_ownership (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_domain      TEXT NOT NULL REFERENCES companies(domain) ON DELETE CASCADE,
    previous_owner_id   TEXT,
    new_owner_id        TEXT,
    change_reason       ownership_change_reason NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ownership_domain ON client_ownership(company_domain);
CREATE INDEX idx_ownership_changed ON client_ownership(changed_at DESC);

-- =============================================================================
-- CAMPAIGN ASSIGNMENTS
-- =============================================================================

CREATE TABLE campaign_assignments (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_email       TEXT NOT NULL,
    contact_client_id   TEXT NOT NULL,
    campaign_id         TEXT NOT NULL,
    client_id           TEXT NOT NULL,
    channel             channel_type NOT NULL DEFAULT 'email',
    assigned_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    outcome             TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_assignments_contact ON campaign_assignments(contact_email, contact_client_id);
CREATE INDEX idx_assignments_campaign ON campaign_assignments(campaign_id);
CREATE INDEX idx_assignments_client ON campaign_assignments(client_id);
CREATE INDEX idx_assignments_active ON campaign_assignments(campaign_id)
    WHERE completed_at IS NULL;

-- =============================================================================
-- TAM SNAPSHOTS
-- =============================================================================

CREATE TABLE tam_snapshots (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_date       DATE NOT NULL,
    client_id           TEXT,

    -- Pool counts
    total_universe      INTEGER NOT NULL DEFAULT 0,
    never_touched       INTEGER NOT NULL DEFAULT 0,
    in_cooldown         INTEGER NOT NULL DEFAULT 0,
    available_now       INTEGER NOT NULL DEFAULT 0,
    permanent_suppress  INTEGER NOT NULL DEFAULT 0,
    in_sequence         INTEGER NOT NULL DEFAULT 0,
    won_customer        INTEGER NOT NULL DEFAULT 0,

    -- Velocity metrics
    burn_rate_weekly    FLOAT,
    exhaustion_eta_weeks FLOAT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(snapshot_date, client_id)
);

CREATE INDEX idx_snapshots_date ON tam_snapshots(snapshot_date DESC);
CREATE INDEX idx_snapshots_client ON tam_snapshots(client_id) WHERE client_id IS NOT NULL;

-- =============================================================================
-- AUTO-CREATE COMPANY TRIGGER
-- =============================================================================

CREATE OR REPLACE FUNCTION auto_create_company()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO companies (domain)
    VALUES (NEW.company_domain)
    ON CONFLICT (domain) DO NOTHING;

    UPDATE companies
    SET contacts_total = contacts_total + 1
    WHERE domain = NEW.company_domain;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_contacts_auto_company
    BEFORE INSERT ON contacts
    FOR EACH ROW EXECUTE FUNCTION auto_create_company();

-- =============================================================================
-- DOCUMENTATION
-- =============================================================================

COMMENT ON TABLE companies IS 'Company-level aggregate state for disposition tracking';
COMMENT ON TABLE contacts IS 'Contact records with disposition state and channel-specific tracking';
COMMENT ON TABLE disposition_history IS 'Append-only audit log of all disposition state transitions';
COMMENT ON TABLE client_ownership IS 'History of company ownership changes for deconfliction audit';
COMMENT ON TABLE campaign_assignments IS 'Tracks which contacts were assigned to which campaigns';
COMMENT ON TABLE tam_snapshots IS 'Periodic snapshots of TAM health metrics for trend analysis';
