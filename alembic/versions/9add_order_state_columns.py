"""Add missing order_state columns for stop/tp/fills."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = "9add_order_state_columns"
down_revision = "8e1acb26fe0f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("order_states") as batch_op:
        batch_op.add_column(sa.Column("stop_order_id", sa.BigInteger(), nullable=True))
        batch_op.add_column(
            sa.Column("take_profit_order_id", sa.BigInteger(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "filled_quantity",
                sa.Numeric(18, 8),
                nullable=False,
                server_default=text("0"),
            )
        )
        batch_op.add_column(
            sa.Column("avg_fill_price", sa.Numeric(18, 8), nullable=True)
        )
        batch_op.add_column(
            sa.Column("last_fill_at", sa.DateTime(timezone=True), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("order_states") as batch_op:
        batch_op.drop_column("last_fill_at")
        batch_op.drop_column("avg_fill_price")
        batch_op.drop_column("filled_quantity")
        batch_op.drop_column("take_profit_order_id")
        batch_op.drop_column("stop_order_id")
