"""Add order_states table for worker persistence"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "8bc02a6f4d3d"
down_revision: Union[str, Sequence[str], None] = "0b4a38b97487"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ORDER_STATUS_ENUM = sa.Enum(
    "armed",
    "pending",
    "filled",
    "cancelled",
    "failed",
    "skipped_low_balance",
    "skipped_whitelist",
    name="order_status_enum",
)


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    _ORDER_STATUS_ENUM.create(bind, checkfirst=True)

    op.create_table(
        "order_states",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "bot_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bots.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("signal_id", sa.String(length=64), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=True),
        sa.Column("status", _ORDER_STATUS_ENUM, nullable=False),
        sa.Column(
            "side",
            sa.Enum("long", "short", "both", name="side_whitelist_enum", create_type=False),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trigger_price", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("stop_price", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("quantity", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("bot_id", "signal_id", name="uq_orderstate_bot_signal"),
    )
    op.create_index("ix_order_states_bot_id", "order_states", ["bot_id"])
    op.create_index("ix_order_states_symbol", "order_states", ["symbol"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_order_states_symbol", table_name="order_states")
    op.drop_index("ix_order_states_bot_id", table_name="order_states")
    op.drop_table("order_states")

    bind = op.get_bind()
    _ORDER_STATUS_ENUM.drop(bind, checkfirst=True)
