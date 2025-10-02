"""Add runtime configuration fields to bots.

Revision ID: 20240305_01
Revises: 0b4a38b97487
Create Date: 2024-03-05 00:00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "20240305_01"
down_revision: Union[str, Sequence[str], None] = "0b4a38b97487"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SIDE_MODE_ENUM = sa.Enum("both", "long_only", "short_only", name="side_mode_enum")
BOT_STATUS_ENUM = sa.Enum("active", "paused", "ended", name="bot_status_enum")


def upgrade() -> None:
    bind = op.get_bind()
    SIDE_MODE_ENUM.create(bind, checkfirst=True)
    BOT_STATUS_ENUM.create(bind, checkfirst=True)

    op.add_column(
        "bots",
        sa.Column("side_mode", SIDE_MODE_ENUM, nullable=False, server_default="both"),
    )
    op.add_column(
        "bots",
        sa.Column("status", BOT_STATUS_ENUM, nullable=False, server_default="active"),
    )
    op.add_column(
        "bots",
        sa.Column("risk_per_trade", sa.Numeric(12, 8), nullable=False, server_default="0.00500000"),
    )
    op.add_column(
        "bots",
        sa.Column("tp_ratio", sa.Numeric(8, 4), nullable=False, server_default="1.5000"),
    )
    op.add_column(
        "bots",
        sa.Column("max_qty", sa.Numeric(18, 8), nullable=True),
    )

    op.execute(
        """
        UPDATE bots
        SET side_mode = CASE
            WHEN side_whitelist = 'long' THEN 'long_only'
            WHEN side_whitelist = 'short' THEN 'short_only'
            ELSE 'both'
        END
        """
    )

    op.execute(
        """
        UPDATE bots
        SET status = CASE
            WHEN enabled IS TRUE THEN 'active'
            ELSE 'paused'
        END
        """
    )


def downgrade() -> None:
    op.drop_column("bots", "max_qty")
    op.drop_column("bots", "tp_ratio")
    op.drop_column("bots", "risk_per_trade")
    op.drop_column("bots", "status")
    op.drop_column("bots", "side_mode")

    bind = op.get_bind()
    BOT_STATUS_ENUM.drop(bind, checkfirst=True)
    SIDE_MODE_ENUM.drop(bind, checkfirst=True)
