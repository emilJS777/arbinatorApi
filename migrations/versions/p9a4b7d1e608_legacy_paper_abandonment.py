"""Explicit unverified abandonment; preserve all execution/history fields."""
from alembic import op
import sqlalchemy as sa

revision = "p9a4b7d1e608"
down_revision = "o8f3a6c0d507"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("strategy_run_trade", sa.Column("abandoned_at", sa.DateTime(), nullable=True))
    op.add_column("strategy_run_trade", sa.Column("abandonment_reason", sa.String(120), nullable=True))
    op.create_index("ix_strategy_run_trade_abandoned_at", "strategy_run_trade", ["abandoned_at"])


def downgrade():
    op.drop_index("ix_strategy_run_trade_abandoned_at", table_name="strategy_run_trade")
    op.drop_column("strategy_run_trade", "abandonment_reason")
    op.drop_column("strategy_run_trade", "abandoned_at")
