-- Pre-deploy S2-1: one poisoned outbox kind must never exist again —
-- the claim path dead-letters unknown kinds per row, and this CHECK
-- stops a bad kind at the write.
ALTER TABLE email_outbox
    ADD CONSTRAINT email_outbox_kind_ok
    CHECK (kind IN ('verify', 'reset', 'invite', 'notice'));
