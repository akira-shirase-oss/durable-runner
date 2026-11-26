-- S3-4: every limit lives in the plans row. The 0002 seed predates
-- three limits the Free plan now owns — retained_bytes (per-ws share
-- of the retained index), invites_per_day (the mail-window cap), and
-- not_before_days (how far a run may park). Existing 'free' rows gain
-- them here; fresh installs could carry them in 0002 but that file is
-- frozen once deployed (CHECKSUMS).
UPDATE plans
SET limits = limits || '{
    "retained_bytes": 16777216,
    "invites_per_day": 20,
    "not_before_days": 30
}'::jsonb
WHERE id = 'free';
