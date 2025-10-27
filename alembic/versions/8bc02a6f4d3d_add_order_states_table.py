# alembic/versions/8bc02a6f4d3d_add_order_states_table.py
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "8bc02a6f4d3d"
down_revision = "0b4a38b97487"
branch_labels = None
depends_on = None

def upgrade():
    # -- Ensure enums exist (create if missing) -------------------------------
    op.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status_enum') THEN
        CREATE TYPE order_status_enum AS ENUM (
          'armed', 'pending', 'filled', 'cancelled', 'failed',
          'skipped_low_balance', 'skipped_whitelist'
        );
      END IF;

      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_side_enum') THEN
        CREATE TYPE order_side_enum AS ENUM ('long','short');
      END IF;
    END$$;
    """)

    # If you need to guarantee labels (no-ops if present)
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'armed';")
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'pending';")
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'filled';")
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'cancelled';")
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'failed';")
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'skipped_low_balance';")
    op.execute("ALTER TYPE order_status_enum ADD VALUE IF NOT EXISTS 'skipped_whitelist';")

    # -- Define enum types for column use WITHOUT creating them again ---------
    status_enum = postgresql.ENUM(
        'armed', 'pending', 'filled', 'cancelled', 'failed',
        'skipped_low_balance', 'skipped_whitelist',
        name='order_status_enum',
        create_type=False,
    )
    side_enum = postgresql.ENUM(
        'long', 'short',
        name='order_side_enum',
        create_type=False,
    )

    # -- Create table using existing enums -----------------------------------
    op.create_table(
        "order_states",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("bot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("signal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("order_id", sa.String(length=64), nullable=True),
        sa.Column("status", status_enum, nullable=False),
        sa.Column("side", side_enum, nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trigger_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("stop_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("bot_id", "signal_id", name="uq_orderstate_bot_signal"),
        schema="public",
    )

    op.create_index(
        "ix_order_states_bot_sym_status",
        "order_states",
        ["bot_id", "symbol", "status"],
        unique=False,
        schema="public",
    )
    op.create_index(
        "ix_order_states_created_at",
        "order_states",
        ["created_at"],
        unique=False,
        schema="public",
    )

def downgrade():
    # Drop table first
    op.drop_index("ix_order_states_created_at", table_name="order_states", schema="public")
    op.drop_index("ix_order_states_bot_sym_status", table_name="order_states", schema="public")
    op.drop_table("order_states", schema="public")

    # Only drop enums if nothing else uses them (safe-ish)
    op.execute("DROP TYPE IF EXISTS order_side_enum")
    op.execute("DROP TYPE IF EXISTS order_status_enum")
