ALTER TABLE conversations DROP CONSTRAINT conversations_kind_check;
ALTER TABLE conversations ADD CONSTRAINT conversations_kind_check CHECK (kind IN ('staff', 'schedule_request', 'report', 'notification'));
ALTER TABLE conversations DROP CONSTRAINT conversations_participants_check;
ALTER TABLE conversations ADD CONSTRAINT conversations_participants_check CHECK (
 (kind = 'staff' AND member_user_id IS NOT NULL AND parent_user_id IS NULL)
 OR (kind IN ('schedule_request', 'report') AND member_user_id IS NULL AND parent_user_id IS NOT NULL)
 OR (kind = 'notification' AND member_user_id IS NULL AND parent_user_id IS NOT NULL AND subject_id = parent_user_id)
);
ALTER TABLE messages DROP CONSTRAINT messages_sender_kind_check;
ALTER TABLE messages ADD CONSTRAINT messages_sender_kind_check CHECK (sender_kind IN ('parent', 'member', 'system'));
ALTER TABLE messages ALTER COLUMN sender_user_id DROP NOT NULL;
ALTER TABLE messages ADD CONSTRAINT messages_sender_shape_check CHECK (
 (sender_kind = 'system' AND sender_user_id IS NULL)
 OR (sender_kind IN ('parent', 'member') AND sender_user_id IS NOT NULL)
);
-- System rows have a NULL sender_user_id, so the original
-- UNIQUE (conversation_id, sender_user_id, client_message_id) can never
-- deduplicate them (NULLs are distinct). This partial index closes that gap
-- for the internal idempotency key without touching the user-facing
-- constraint or requiring a NULLS NOT DISTINCT-capable server.
CREATE UNIQUE INDEX messages_system_idempotency ON messages(conversation_id, client_message_id) WHERE sender_kind = 'system';
