"""add futures trend paper trading tables

Revision ID: 5c2f8a9d0e44
Revises: 4b9c7d2e1a33
Create Date: 2026-06-09 00:00:02.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "5c2f8a9d0e44"
down_revision = "4b9c7d2e1a33"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "candle",
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_candle_market_time", "candle", ["exchange", "symbol", "timeframe", "timestamp"], unique=True)
    op.create_table(
        "equity_curve_point",
        sa.Column("equity", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "futures_trade",
        sa.Column("position_id", sa.Integer(), nullable=True),
        sa.Column("signal_id", sa.Integer(), nullable=True),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("side", sa.String(length=20), nullable=False),
        sa.Column("entry_price", sa.Float(), nullable=False),
        sa.Column("exit_price", sa.Float(), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("leverage", sa.Float(), nullable=False),
        sa.Column("margin", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False),
        sa.Column("exit_reason", sa.String(length=40), nullable=True),
        sa.Column("opened_at", sa.DateTime(), nullable=True),
        sa.Column("closed_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["position_id"], ["paper_position.id"]),
        sa.ForeignKeyConstraint(["signal_id"], ["trade_signal.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("paper_position", schema=None) as batch_op:
        batch_op.add_column(sa.Column("margin_mode", sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column("liquidation_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("take_profit_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("stop_loss_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("exit_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("exit_reason", sa.String(length=40), nullable=True))
        batch_op.add_column(sa.Column("strategy_type", sa.String(length=80), nullable=True))


def downgrade():
    with op.batch_alter_table("paper_position", schema=None) as batch_op:
        batch_op.drop_column("strategy_type")
        batch_op.drop_column("exit_reason")
        batch_op.drop_column("exit_price")
        batch_op.drop_column("stop_loss_price")
        batch_op.drop_column("take_profit_price")
        batch_op.drop_column("liquidation_price")
        batch_op.drop_column("margin_mode")
    op.drop_table("futures_trade")
    op.drop_table("equity_curve_point")
    op.drop_index("ix_candle_market_time", table_name="candle")
    op.drop_table("candle")
