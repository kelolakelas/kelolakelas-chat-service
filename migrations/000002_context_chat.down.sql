-- Refuse a destructive rollback while context conversations exist.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM conversations WHERE kind <> 'staff') THEN
  RAISE EXCEPTION 'context conversations must be retained; rollback refused';
 END IF;
END $$;
DROP INDEX conversations_parent_recent;
ALTER TABLE conversations DROP CONSTRAINT conversations_participants_check;
ALTER TABLE conversations ALTER COLUMN member_user_id SET NOT NULL;
ALTER TABLE conversations DROP CONSTRAINT conversations_kind_check;
ALTER TABLE conversations ADD CONSTRAINT conversations_kind_check CHECK (kind = 'staff');
