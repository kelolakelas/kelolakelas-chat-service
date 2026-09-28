CREATE TABLE conversations (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, kind text NOT NULL CHECK (kind = 'staff'), subject_id uuid NOT NULL,
 parent_user_id uuid, member_user_id uuid NOT NULL, context jsonb NOT NULL DEFAULT '{}'::jsonb,
 created_by_user_id uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), last_message_at timestamptz,
 UNIQUE (tenant_id, kind, subject_id)
);
CREATE TABLE messages (
 id uuid PRIMARY KEY, conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
 sender_user_id uuid NOT NULL, sender_kind text NOT NULL CHECK (sender_kind IN ('parent','member')),
 body text NOT NULL CHECK (char_length(body) BETWEEN 1 AND 2000), client_message_id text NOT NULL CHECK (client_message_id <> ''),
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE (conversation_id, sender_user_id, client_message_id)
);
CREATE INDEX messages_timeline ON messages(conversation_id, created_at DESC, id DESC);
CREATE TABLE conversation_reads (
 conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
 user_id uuid NOT NULL, last_read_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX conversations_tenant_recent ON conversations(tenant_id, last_message_at DESC NULLS LAST, created_at DESC, id DESC);
