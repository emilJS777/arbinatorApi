"""Durable execution reservation and bounded risk; no historical rewrites."""
from alembic import op
import sqlalchemy as sa

revision = "m6d1e4a8b305"
down_revision = "l5c9e3a7b204"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch:
        for name, kind, default in [
            ("risk_per_trade_percent", sa.Float(), "0.25"),
            ("max_position_margin_usdt", sa.Float(), "10"),
            ("emergency_entry_block", sa.Boolean(), sa.true()),
            ("paper_taker_fee_percent", sa.Float(), "0.1"),
            ("paper_latency_ms", sa.Integer(), "250"),
            ("max_consecutive_losses", sa.Integer(), "3"),
            ("max_leverage", sa.Float(), "2"),
        ]:
            batch.add_column(sa.Column(name, kind, nullable=False, server_default=default))
    with op.batch_alter_table("strategy_run_trade") as batch:
        batch.add_column(sa.Column("execution_config_json", sa.Text(), nullable=True))
        batch.add_column(sa.Column("live_client_order_id", sa.String(80), nullable=True))
        batch.add_column(sa.Column("live_close_client_order_id", sa.String(80), nullable=True))
        batch.add_column(sa.Column("paper_close_requested_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("paper_close_reason", sa.String(80), nullable=True))
    op.create_table(
        "orderbook_execution_slot",
        sa.Column("strategy_config_id", sa.Integer(), sa.ForeignKey("order_book_pattern_strategy_config.id"), primary_key=True),
        sa.Column("trade_id", sa.Integer(), sa.ForeignKey("strategy_run_trade.id")),
        sa.Column("client_order_id", sa.String(80), nullable=False, unique=True),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )


def downgrade():
    op.drop_table("orderbook_execution_slot")
    with op.batch_alter_table("strategy_run_trade") as batch:
        for name in ("paper_close_reason", "paper_close_requested_at", "live_close_client_order_id", "live_client_order_id", "execution_config_json"):
            batch.drop_column(name)
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch:
        for name in ("max_leverage", "max_consecutive_losses", "paper_latency_ms", "paper_taker_fee_percent", "emergency_entry_block", "max_position_margin_usdt", "risk_per_trade_percent"):
            batch.drop_column(name)
