"""add backend arbitrage tables

Revision ID: 4b9c7d2e1a33
Revises: 3f8a2b7c9d01
Create Date: 2026-06-09 00:00:01.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "4b9c7d2e1a33"
down_revision = "3f8a2b7c9d01"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "arbitrage_config",
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("symbols_allowlist", sa.JSON(), nullable=True),
        sa.Column("exchanges_allowlist", sa.JSON(), nullable=True),
        sa.Column("min_spread_percent", sa.Float(), nullable=False),
        sa.Column("min_net_profit_percent", sa.Float(), nullable=False),
        sa.Column("min_profit_usdt", sa.Float(), nullable=False),
        sa.Column("max_order_margin_usdt", sa.Float(), nullable=False),
        sa.Column("max_leverage", sa.Float(), nullable=False),
        sa.Column("taker_fee_buffer_percent", sa.Float(), nullable=False),
        sa.Column("slippage_buffer_percent", sa.Float(), nullable=False),
        sa.Column("cooldown_seconds_per_symbol", sa.Integer(), nullable=False),
        sa.Column("paper_execute_enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "arbitrage_opportunity",
        sa.Column("signal_id", sa.Integer(), nullable=True),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("buy_exchange", sa.String(length=80), nullable=False),
        sa.Column("sell_exchange", sa.String(length=80), nullable=False),
        sa.Column("buy_price", sa.Float(), nullable=False),
        sa.Column("sell_price", sa.Float(), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("gross_spread_percent", sa.Float(), nullable=False),
        sa.Column("net_profit_percent", sa.Float(), nullable=False),
        sa.Column("expected_profit_usdt", sa.Float(), nullable=False),
        sa.Column("total_cost_usdt", sa.Float(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("dedupe_key", sa.String(length=180), nullable=False),
        sa.Column("config_snapshot", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["signal_id"], ["trade_signal.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("trade_signal", schema=None) as batch_op:
        batch_op.add_column(sa.Column("strategy_type", sa.String(length=80), nullable=True))
        batch_op.add_column(sa.Column("buy_exchange", sa.String(length=80), nullable=True))
        batch_op.add_column(sa.Column("sell_exchange", sa.String(length=80), nullable=True))
        batch_op.add_column(sa.Column("buy_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("sell_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("gross_spread_percent", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("net_profit_percent", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("expected_profit_usdt", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("config_snapshot", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("dedupe_key", sa.String(length=180), nullable=True))
        batch_op.create_index("ix_trade_signal_dedupe_key", ["dedupe_key"], unique=False)
    op.create_index("ix_arbitrage_opportunity_dedupe_key", "arbitrage_opportunity", ["dedupe_key"], unique=False)


def downgrade():
    op.drop_index("ix_arbitrage_opportunity_dedupe_key", table_name="arbitrage_opportunity")
    with op.batch_alter_table("trade_signal", schema=None) as batch_op:
        batch_op.drop_index("ix_trade_signal_dedupe_key")
        batch_op.drop_column("dedupe_key")
        batch_op.drop_column("config_snapshot")
        batch_op.drop_column("expected_profit_usdt")
        batch_op.drop_column("net_profit_percent")
        batch_op.drop_column("gross_spread_percent")
        batch_op.drop_column("sell_price")
        batch_op.drop_column("buy_price")
        batch_op.drop_column("sell_exchange")
        batch_op.drop_column("buy_exchange")
        batch_op.drop_column("strategy_type")
    op.drop_table("arbitrage_opportunity")
    op.drop_table("arbitrage_config")
