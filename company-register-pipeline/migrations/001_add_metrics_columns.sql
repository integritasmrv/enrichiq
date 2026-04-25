-- Migration: Add rows_inserted, rows_updated, status to pipeline_metrics
-- Date: 2026-04-25

ALTER TABLE public.pipeline_metrics 
ADD COLUMN IF NOT EXISTS rows_inserted BIGINT DEFAULT 0,
ADD COLUMN IF NOT EXISTS rows_updated BIGINT DEFAULT 0,
ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'completed';

COMMENT ON COLUMN public.pipeline_metrics.rows_inserted IS 'Number of rows inserted during operation';
COMMENT ON COLUMN public.pipeline_metrics.rows_updated IS 'Number of rows updated during operation';
COMMENT ON COLUMN public.pipeline_metrics.status IS 'Status of operation: completed, failed, skipped';