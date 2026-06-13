"""orderbook recovery signal diagnostics limit

Revision ID: g8d5e2f1a904
Revises: f7a4d8c2e901
Create Date: 2026-06-13 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "g8d5e2f1a904"
down_revision = "f7a4d8c2e901"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "order_book_pattern_strategy_config",
        sa.Column("signal_diagnostics_max_rows", sa.Integer(), nullable=False, server_default="100"),
    )


def downgrade():
    op.drop_column("order_book_pattern_strategy_config", "signal_diagnostics_max_rows")
