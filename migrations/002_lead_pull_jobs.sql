-- Migration: Add lead_pull_jobs table to Charm Email OS database
-- Follows the same job queue pattern as domain_generation_jobs,
-- strategy_generation_jobs, and inbox_purchase_jobs.
--
-- Run against the CHARM database, not the disposition database.

CREATE TABLE IF NOT EXISTS lead_pull_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES clients(id),
    suggestion_id UUID REFERENCES strategy_suggestions(id),
    submission_id UUID REFERENCES client_onboarding_submissions(id),

    -- Fill parameters
    volume INTEGER NOT NULL DEFAULT 500,
    channel VARCHAR(50) DEFAULT 'email',
    max_external_credits FLOAT DEFAULT 100.0,
    enable_external BOOLEAN DEFAULT TRUE,

    -- Search criteria (built from onboarding data at job creation time)
    search_criteria JSONB NOT NULL DEFAULT '{}',

    -- Job status (matches Charm pattern: pending -> processing -> completed/failed)
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,

    -- Result from WaterfallFillResult
    result_data JSONB,

    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_lead_pull_jobs_status ON lead_pull_jobs(status);
CREATE INDEX idx_lead_pull_jobs_client ON lead_pull_jobs(client_id);

-- Trigger: auto-create a lead_pull_job when a strategy_suggestion is approved.
-- Reads the client's onboarding data to populate search_criteria.
CREATE OR REPLACE FUNCTION fn_create_lead_pull_job()
RETURNS TRIGGER AS $$
DECLARE
    v_submission_id UUID;
    v_criteria JSONB;
    v_titles JSONB;
    v_signals JSONB;
    v_target TEXT;
    v_pain_points TEXT[];
    v_persona_titles TEXT[];
BEGIN
    -- Only fire when status changes TO 'approved'
    IF NEW.status = 'approved' AND (OLD.status IS NULL OR OLD.status != 'approved') THEN

        -- Get the active onboarding submission for this client
        SELECT id, job_titles, signals, target_customer
        INTO v_submission_id, v_titles, v_signals, v_target
        FROM client_onboarding_submissions
        WHERE client_id = NEW.client_id AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1;

        -- Get persona job titles
        IF v_submission_id IS NOT NULL THEN
            SELECT array_agg(job_title)
            INTO v_persona_titles
            FROM client_personas
            WHERE submission_id = v_submission_id;
        END IF;

        -- Get segment pain points for search keywords
        IF v_submission_id IS NOT NULL THEN
            SELECT array_agg(pain_points)
            INTO v_pain_points
            FROM client_segments
            WHERE submission_id = v_submission_id
              AND pain_points IS NOT NULL;
        END IF;

        -- Build search criteria JSON
        v_criteria := jsonb_build_object(
            'title_keywords', COALESCE(v_titles, '[]'::jsonb),
            'persona_titles', COALESCE(to_jsonb(v_persona_titles), '[]'::jsonb),
            'industry', COALESCE(v_target, ''),
            'search_keywords', COALESCE(to_jsonb(v_pain_points), '[]'::jsonb),
            'signals', COALESCE(v_signals, '[]'::jsonb),
            'campaign_type', COALESCE(NEW.campaign_type, ''),
            'subject_line', COALESCE(NEW.subject_line, ''),
            'variant_number', COALESCE(NEW.variant_number, 1)
        );

        INSERT INTO lead_pull_jobs (
            client_id, suggestion_id, submission_id,
            volume, search_criteria, status
        ) VALUES (
            NEW.client_id, NEW.id, v_submission_id,
            500, v_criteria, 'pending'
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_strategy_approved_lead_pull
    AFTER UPDATE ON strategy_suggestions
    FOR EACH ROW
    EXECUTE FUNCTION fn_create_lead_pull_job();
