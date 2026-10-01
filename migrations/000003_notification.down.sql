-- Refuse a destructive rollback while system notification data exists.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM messages WHERE sender_kind = 'system') OR EXISTS (SELECT 1 FROM conversations WHERE kind = 'notification') THEN
  RAISE EXCEPTION 'system notification data must be retained; rollback refused';
 END IF;
END $$;
DROP INDEX messages_system_idempotency;
ALTER TABLE messages DROP CONSTRAINT messages_sender_shape_check;
ALTER TABLE messages ALTER COLUMN sender_user_id SET NOT NULL;
ALTER TABLE messages DROP CONSTRAINT messages_sender_kind_check;
ALTER TABLE messages ADD CONSTRAINT messages_sender_kind_check CHECK (sender_kind IN ('parent', 'member'));
ALTER TABLE conversations DROP CONSTRAINT conversations_participants_check;
ALTER TABLE conversations ADD CONSTRAINT conversations_participants_check CHECK (
 (kind = 'staff' AND member_user_id IS NOT NULL AND parent_user_id IS NULL)
 OR (kind IN ('schedule_request', 'report') AND member_user_id IS NULL AND parent_user_id IS NOT NULL)
);
ALTER TABLE conversations DROP CONSTRAINT conversations_kind_check;
ALTER TABLE conversations ADD CONSTRAINT conversations_kind_check CHECK (kind IN ('staff', 'schedule_request', 'report'));
