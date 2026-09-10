CREATE TABLE polls (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    question text NOT NULL CHECK (char_length(question) BETWEEN 1 AND 300),
    kind text NOT NULL CHECK (kind IN ('ab', 'single', 'multiple')),
    options jsonb NOT NULL CHECK (jsonb_typeof(options) = 'array' AND jsonb_array_length(options) BETWEEN 2 AND 20),
    starts_at timestamptz NOT NULL,
    ends_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finalized_at timestamptz,
    CHECK (ends_at = starts_at + interval '60 seconds')
);
CREATE INDEX polls_due ON polls (ends_at) WHERE finalized_at IS NULL;

-- The token, not poll_id alone, distributes one large poll across partitions.
-- Both key columns are included in the unique constraint. Tokens are random
-- browser state scoped to a poll; they are not proof of a unique person.
CREATE TABLE votes (
    poll_id uuid NOT NULL REFERENCES polls(id),
    token uuid NOT NULL,
    choices integer[] NOT NULL CHECK (cardinality(choices) BETWEEN 1 AND 20),
    accepted_at timestamptz NOT NULL,
    PRIMARY KEY (poll_id, token)
) PARTITION BY HASH (token);
DO $$
BEGIN
    FOR shard IN 0..31 LOOP
        EXECUTE format('CREATE TABLE votes_p%s PARTITION OF votes FOR VALUES WITH (MODULUS 32, REMAINDER %s)', shard, shard);
    END LOOP;
END $$;

CREATE TABLE poll_results (
    poll_id uuid PRIMARY KEY REFERENCES polls(id),
    total_votes bigint NOT NULL CHECK (total_votes >= 0),
    options jsonb NOT NULL,
    calculated_at timestamptz NOT NULL
);

-- Advisory locks use the two-int key space; migration/creation locks use the
-- separate bigint key space. Hash collisions can reduce concurrency, but do
-- not merge votes or weaken the finalization barrier.
CREATE FUNCTION vote_lock_shard(p_token uuid) RETURNS integer
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT ((hashtext(p_token::text)::bigint & 2147483647) % 64)::integer
$$;

-- This must remain VOLATILE and run at READ COMMITTED: after ON CONFLICT waits
-- for another transaction, the next SELECT needs a fresh snapshot of its row.
CREATE FUNCTION record_vote(p_poll_id uuid, p_token uuid, p_choices integer[])
RETURNS TABLE(status text, choices integer[], accepted_at timestamptz)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    config polls%ROWTYPE;
    previous votes%ROWTYPE;
    normalized integer[];
    admitted_at timestamptz;
    exclusive_receipt_lookup boolean;
BEGIN
    SELECT * INTO config FROM polls WHERE id = p_poll_id;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'not_found'::text, NULL::integer[], NULL::timestamptz;
        RETURN;
    END IF;

    -- A late receipt lookup drains the shard, so an uncommitted old vote
    -- cannot be mistaken for a definitive absence. Never upgrade a held lock.
    exclusive_receipt_lookup := clock_timestamp() >= config.ends_at OR config.finalized_at IS NOT NULL;
    IF exclusive_receipt_lookup THEN
        PERFORM pg_advisory_xact_lock(hashtext(p_poll_id::text), vote_lock_shard(p_token));
    ELSE
        PERFORM pg_advisory_xact_lock_shared(hashtext(p_poll_id::text), vote_lock_shard(p_token));
    END IF;

    SELECT * INTO config FROM polls WHERE id = p_poll_id;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'not_found'::text, NULL::integer[], NULL::timestamptz;
        RETURN;
    END IF;

    IF p_token IS NULL OR p_choices IS NULL OR array_ndims(p_choices) IS DISTINCT FROM 1
       OR cardinality(p_choices) NOT BETWEEN 1 AND 20
       OR array_position(p_choices, NULL) IS NOT NULL THEN
        RETURN QUERY SELECT 'invalid'::text, NULL::integer[], NULL::timestamptz;
        RETURN;
    END IF;
    SELECT array_agg(value ORDER BY value) INTO normalized FROM unnest(p_choices) AS value;
    IF (SELECT count(DISTINCT value) FROM unnest(normalized) AS value) <> cardinality(normalized)
       OR (config.kind <> 'multiple' AND cardinality(normalized) <> 1)
       OR EXISTS (
           SELECT 1 FROM unnest(normalized) AS selected(option_id)
           WHERE NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(config.options) AS option
               WHERE (option->>'id')::integer = selected.option_id
           )
       ) THEN
        RETURN QUERY SELECT 'invalid'::text, NULL::integer[], NULL::timestamptz;
        RETURN;
    END IF;

    -- The authoritative instant follows the lock wait and input validation.
    -- An admitted operation may commit after ends_at while holding its lock.
    admitted_at := clock_timestamp();
    IF config.finalized_at IS NOT NULL OR admitted_at >= config.ends_at OR admitted_at < config.starts_at THEN
        SELECT * INTO previous FROM votes AS v WHERE v.poll_id = p_poll_id AND v.token = p_token;
        IF FOUND THEN
            RETURN QUERY SELECT CASE WHEN previous.choices = normalized THEN 'duplicate' ELSE 'conflict' END,
                                previous.choices, previous.accepted_at;
            RETURN;
        END IF;
        IF config.finalized_at IS NOT NULL THEN
            RETURN QUERY SELECT 'closed'::text, NULL::integer[], NULL::timestamptz;
        ELSIF admitted_at >= config.ends_at THEN
            -- A deadline crossing after choosing a shared lock can leave an
            -- earlier transaction in flight. The next late retry drains it.
            RETURN QUERY SELECT CASE WHEN exclusive_receipt_lookup THEN 'closed' ELSE 'unknown' END,
                                NULL::integer[], NULL::timestamptz;
        ELSE
            RETURN QUERY SELECT 'not_open'::text, NULL::integer[], NULL::timestamptz;
        END IF;
        RETURN;
    END IF;

    -- New voters need only the unique-index check performed by INSERT; avoid
    -- a separate indexed existence lookup on the normal open-window path.
    INSERT INTO votes AS v (poll_id, token, choices, accepted_at)
    VALUES (p_poll_id, p_token, normalized, admitted_at)
    ON CONFLICT (poll_id, token) DO NOTHING
    RETURNING v.* INTO previous;
    IF FOUND THEN
        RETURN QUERY SELECT 'accepted'::text, previous.choices, previous.accepted_at;
        RETURN;
    END IF;

    -- A concurrent winner may have been invisible to the INSERT snapshot.
    -- VOLATILE/READ COMMITTED gives this separate command its committed row.
    SELECT * INTO STRICT previous FROM votes AS v WHERE v.poll_id = p_poll_id AND v.token = p_token;
    RETURN QUERY SELECT CASE WHEN previous.choices = normalized THEN 'duplicate' ELSE 'conflict' END,
                        previous.choices, previous.accepted_at;
END $$;

CREATE FUNCTION calculate_option_counts(p_poll_id uuid, p_options jsonb) RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT coalesce(jsonb_agg(jsonb_build_object(
        'id', (option->>'id')::integer,
        'label', option->>'label',
        'votes', coalesce(tally.n, 0)
    ) ORDER BY (option->>'id')::integer), '[]'::jsonb)
    FROM jsonb_array_elements(p_options) AS option
    LEFT JOIN (
        SELECT choice, count(*) AS n
        FROM votes AS v CROSS JOIN LATERAL unnest(v.choices) AS choice
        WHERE v.poll_id = p_poll_id
        GROUP BY choice
    ) AS tally ON tally.choice = (option->>'id')::integer
$$;

CREATE FUNCTION finalize_poll(p_poll_id uuid) RETURNS boolean
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    config polls%ROWTYPE;
    finalized timestamptz;
    total bigint;
    counts jsonb;
BEGIN
    SELECT * INTO config FROM polls WHERE id = p_poll_id;
    IF NOT FOUND OR config.finalized_at IS NOT NULL OR clock_timestamp() < config.ends_at THEN
        RETURN false;
    END IF;

    -- Fixed ordering across every caller avoids lock cycles. No poll row is
    -- locked before draining votes (their FK checks can hold row key locks).
    FOR shard IN 0..63 LOOP
        PERFORM pg_advisory_xact_lock(hashtext(p_poll_id::text), shard);
    END LOOP;
    SELECT * INTO config FROM polls WHERE id = p_poll_id FOR UPDATE;
    IF NOT FOUND OR config.finalized_at IS NOT NULL THEN
        RETURN false;
    END IF;
    SELECT count(*) INTO total FROM votes WHERE poll_id = p_poll_id;
    SELECT calculate_option_counts(p_poll_id, config.options) INTO counts;
    finalized := clock_timestamp();
    INSERT INTO poll_results (poll_id, total_votes, options, calculated_at)
    VALUES (p_poll_id, total, counts, finalized);
    UPDATE polls SET finalized_at = finalized WHERE id = p_poll_id;
    RETURN true;
END $$;
