"""Protection monitoring and funding ledger; preserve applied migrations."""
from alembic import op
import sqlalchemy as sa

revision = "n7e2f5b9c406"
down_revision = "m6d1e4a8b305"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("strategy_run_trade") as batch:
        for name, kind in [
            ("protection_status", sa.String(40)), ("protection_checked_at", sa.DateTime()),
            ("protection_expires_at", sa.DateTime()), ("legacy_reconciliation_status", sa.String(80)),
            ("funding_pnl", sa.Float()), ("funding_status", sa.String(40)), ("funding_checked_at", sa.DateTime()),
        ]:
            batch.add_column(sa.Column(name, kind, nullable=True))
        batch.create_index("ix_strategy_run_trade_funding_status", ["funding_status"])
    op.create_table("orderbook_trade_funding_event",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_id", sa.Integer(), sa.ForeignKey("strategy_run_trade.id", ondelete="CASCADE"), nullable=False),
        sa.Column("exchange_event_id", sa.String(120), nullable=False),
        sa.Column("settled_at", sa.DateTime(), nullable=False),
        sa.Column("amount_usdt", sa.Float(), nullable=False),
        sa.UniqueConstraint("trade_id", "exchange_event_id"))
    op.create_index("ix_orderbook_trade_funding_event_trade_id", "orderbook_trade_funding_event", ["trade_id"])


def downgrade():
    op.drop_table("orderbook_trade_funding_event")
    with op.batch_alter_table("strategy_run_trade") as batch:
        batch.drop_index("ix_strategy_run_trade_funding_status")
        for name in ("funding_checked_at", "funding_status", "funding_pnl", "legacy_reconciliation_status", "protection_expires_at", "protection_checked_at", "protection_status"):
            batch.drop_column(name)
