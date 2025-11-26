"""sync order_states enum types

Revision ID: 8e1acb26fe0f
Revises: 8bc02a6f4d3d
Create Date: 2025-10-28 20:29:37.379239
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "8e1acb26fe0f"
down_revision: Union[str, Sequence[str], None] = "8bc02a6f4d3d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # ---------------------------------------------------------------------
    # CUSTOM PATCH: new enum + casts (must run before autogen changes)
    # ---------------------------------------------------------------------

    # 1) Ensure the new enum exists
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_side_enum') THEN
                CREATE TYPE order_side_enum AS ENUM ('long', 'short');
            END IF;
        END$$;
        """
    )

    # 2) Ensure bot_id is UUID (fixes "uuid = character varying" comparison errors)
    #    If column is already UUID, this remains a no-op conversion.
    op.execute(
        """
        ALTER TABLE order_states
        ALTER COLUMN bot_id TYPE uuid
        USING bot_id::uuid
        """
    )

    # 3) Normalize any legacy/invalid values in side (e.g., 'both') so cast is safe
    op.execute(
        """
        UPDATE order_states
        SET side = 'long'
        WHERE side NOT IN ('long', 'short');
        """
    )

    # 4) Cast side from text/old-enum to new enum
    op.execute(
        """
        ALTER TABLE order_states
        ALTER COLUMN side TYPE order_side_enum
        USING side::text::order_side_enum
        """
    )

    # ---------------------------------------------------------------------
    # AUTOGEN SECTION (kept as originally generated)
    # ---------------------------------------------------------------------
    op.drop_index(op.f("bots_backup_cred_id_idx"), table_name="bots_backup")
    op.drop_index(op.f("bots_backup_symbol_enabled_idx"), table_name="bots_backup")
    op.drop_index(op.f("bots_backup_symbol_idx"), table_name="bots_backup")
    op.drop_index(op.f("bots_backup_user_id_enabled_idx"), table_name="bots_backup")
    op.drop_index(op.f("bots_backup_user_id_idx"), table_name="bots_backup")
    op.drop_table("bots_backup")

    op.alter_column(
        "order_states",
        "order_id",
        type_=sa.BigInteger(),
        existing_type=sa.VARCHAR(length=64),
        existing_nullable=True,
        postgresql_using="order_id::bigint",
    )

    op.alter_column(
        "order_states",
        "signal_id",
        existing_type=sa.UUID(),
        type_=sa.String(length=64),
        nullable=False,
    )
    op.alter_column(
        "order_states",
        "order_id",
        existing_type=sa.VARCHAR(length=64),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.alter_column(
        "order_states",
        "trigger_price",
        existing_type=sa.NUMERIC(precision=38, scale=18),
        type_=sa.Numeric(precision=18, scale=8),
        nullable=False,
    )
    op.alter_column(
        "order_states",
        "stop_price",
        existing_type=sa.NUMERIC(precision=38, scale=18),
        type_=sa.Numeric(precision=18, scale=8),
        nullable=False,
    )
    op.alter_column(
        "order_states",
        "quantity",
        existing_type=sa.NUMERIC(precision=38, scale=18),
        type_=sa.Numeric(precision=18, scale=8),
        nullable=False,
    )
    op.drop_index(op.f("ix_order_states_bot_sym_status"), table_name="order_states")
    op.drop_index(op.f("ix_order_states_created_at"), table_name="order_states")
    op.create_index(
        op.f("ix_order_states_bot_id"), "order_states", ["bot_id"], unique=False
    )
    op.create_index(
        op.f("ix_order_states_symbol"), "order_states", ["symbol"], unique=False
    )
    op.create_foreign_key(
        op.f("fk_order_states_bot_id_bots"),
        "order_states",
        "bots",
        ["bot_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Downgrade schema."""

    # ---------------------------------------------------------------------
    # CUSTOM PATCH: revert side enum + optional bot_id cast back
    # ---------------------------------------------------------------------

    # 1) Recreate old enum and cast side back to it (to truly support downgrade)
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'side_whitelist_enum') THEN
                CREATE TYPE side_whitelist_enum AS ENUM ('long', 'short', 'both');
            END IF;
        END$$;
        """
    )

    op.execute(
        """
        ALTER TABLE order_states
        ALTER COLUMN side TYPE side_whitelist_enum
        USING side::text::side_whitelist_enum
        """
    )

    # 2) Optionally revert bot_id back to varchar (if your pre-migration state used varchar)
    #    If you prefer keeping UUID on downgrade, comment this out.
    op.execute(
        """
        ALTER TABLE order_states
        ALTER COLUMN bot_id TYPE varchar
        USING bot_id::text
        """
    )

    # 3) Drop new enum if not used anymore (guarded drop; won't error if still referenced)
    op.execute(
        """
        DO $$
        BEGIN
            -- Only drop if there are no dependencies
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type t
                JOIN pg_depend d ON d.refobjid = t.oid
                WHERE t.typname = 'order_side_enum' AND d.deptype = 'e'
            ) THEN
                DROP TYPE IF EXISTS order_side_enum;
            END IF;
        END$$;
        """
    )

    # ---------------------------------------------------------------------
    # AUTOGEN SECTION (kept as originally generated)
    # ---------------------------------------------------------------------
    op.drop_constraint(
        op.f("fk_order_states_bot_id_bots"), "order_states", type_="foreignkey"
    )
    op.drop_index(op.f("ix_order_states_symbol"), table_name="order_states")
    op.drop_index(op.f("ix_order_states_bot_id"), table_name="order_states")
    op.create_index(
        op.f("ix_order_states_created_at"), "order_states", ["created_at"], unique=False
    )
    op.create_index(
        op.f("ix_order_states_bot_sym_status"),
        "order_states",
        ["bot_id", "symbol", "status"],
        unique=False,
    )
    op.alter_column(
        "order_states",
        "quantity",
        existing_type=sa.Numeric(precision=18, scale=8),
        type_=sa.NUMERIC(precision=38, scale=18),
        nullable=True,
    )
    op.alter_column(
        "order_states",
        "stop_price",
        existing_type=sa.Numeric(precision=18, scale=8),
        type_=sa.NUMERIC(precision=38, scale=18),
        nullable=True,
    )
    op.alter_column(
        "order_states",
        "trigger_price",
        existing_type=sa.Numeric(precision=18, scale=8),
        type_=sa.NUMERIC(precision=38, scale=18),
        nullable=True,
    )
    op.alter_column(
        "order_states",
        "order_id",
        existing_type=sa.BigInteger(),
        type_=sa.VARCHAR(length=64),
        existing_nullable=True,
    )
    op.alter_column(
        "order_states",
        "signal_id",
        existing_type=sa.String(length=64),
        type_=sa.UUID(),
        nullable=True,
    )
    op.alter_column(
        "order_states",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_nullable=False,
    )
    op.create_table(
        "bots_backup",
        sa.Column("id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column("user_id", sa.UUID(), autoincrement=False, nullable=True),
        sa.Column("cred_id", sa.UUID(), autoincrement=False, nullable=True),
        sa.Column("symbol", sa.VARCHAR(length=32), autoincrement=False, nullable=False),
        sa.Column(
            "timeframe", sa.VARCHAR(length=8), autoincrement=False, nullable=False
        ),
        sa.Column("enabled", sa.BOOLEAN(), autoincrement=False, nullable=False),
        sa.Column(
            "env",
            postgresql.ENUM("testnet", "prod", name="env_enum"),
            server_default=sa.text("'testnet'::env_enum"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "side_whitelist",
            postgresql.ENUM("long", "short", "both", name="side_whitelist_enum"),
            server_default=sa.text("'both'::side_whitelist_enum"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("leverage", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("use_balance_pct", sa.BOOLEAN(), autoincrement=False, nullable=False),
        sa.Column(
            "balance_pct",
            sa.NUMERIC(precision=6, scale=4),
            server_default=sa.text("0.0500"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "fixed_notional",
            sa.NUMERIC(precision=18, scale=4),
            server_default=sa.text("0.0000"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "max_position_usdt",
            sa.NUMERIC(precision=18, scale=4),
            server_default=sa.text("0.0000"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "nickname", sa.VARCHAR(length=64), autoincrement=False, nullable=False
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("bots_backup_pkey")),
    )
    op.create_index(
        op.f("bots_backup_user_id_idx"), "bots_backup", ["user_id"], unique=False
    )
    op.create_index(
        op.f("bots_backup_user_id_enabled_idx"),
        "bots_backup",
        ["user_id", "enabled"],
        unique=False,
    )
    op.create_index(
        op.f("bots_backup_symbol_idx"), "bots_backup", ["symbol"], unique=False
    )
    op.create_index(
        op.f("bots_backup_symbol_enabled_idx"),
        "bots_backup",
        ["symbol", "enabled"],
        unique=False,
    )
    op.create_index(
        op.f("bots_backup_cred_id_idx"), "bots_backup", ["cred_id"], unique=False
    )
