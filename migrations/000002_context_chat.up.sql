ALTER TABLE conversations DROP CONSTRAINT conversations_kind_check;
ALTER TABLE conversations ADD CONSTRAINT conversations_kind_check CHECK (kind IN ('staff', 'schedule_request', 'report'));
ALTER TABLE conversations ALTER COLUMN member_user_id DROP NOT NULL;
ALTER TABLE conversations ADD CONSTRAINT conversations_participants_check CHECK (
 (kind = 'staff' AND member_user_id IS NOT NULL AND parent_user_id IS NULL)
 OR (kind IN ('schedule_request', 'report') AND member_user_id IS NULL AND parent_user_id IS NOT NULL)
);
CREATE INDEX conversations_parent_recent ON conversations(parent_user_id, last_message_at DESC NULLS LAST, created_at DESC, id DESC) WHERE parent_user_id IS NOT NULL;
