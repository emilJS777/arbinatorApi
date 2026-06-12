"""add order book recovery live execution mode

Revision ID: d1a7c8e9f204
Revises: c9d2e4f7a105
Create Date: 2026-06-12
"""
from alembic import op
import sqlalchemy as sa


revision = "d1a7c8e9f204"
down_revision = "c9d2e4f7a105"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("execution_mode", sa.String(length=20), nullable=False, server_default="paper"))
        batch_op.add_column(sa.Column("live_enabled_confirmation", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch_op.add_column(sa.Column("live_kill_switch", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.add_column(sa.Column("live_max_margin_usdt", sa.Float(), nullable=False, server_default="10"))
        batch_op.add_column(sa.Column("live_max_daily_loss_usdt", sa.Float(), nullable=False, server_default="5"))
        batch_op.add_column(sa.Column("live_max_total_loss_usdt", sa.Float(), nullable=False, server_default="10"))
        batch_op.add_column(sa.Column("live_order_type", sa.String(length=20), nullable=False, server_default="market"))
        batch_op.add_column(sa.Column("live_reduce_only_close", sa.Boolean(), nullable=False, server_default=sa.true()))

    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.add_column(sa.Column("execution_mode", sa.String(length=20), nullable=True, server_default="paper"))
        batch_op.add_column(sa.Column("live_exchange_order_id", sa.String(length=120), nullable=True))
        batch_op.add_column(sa.Column("live_close_order_id", sa.String(length=120), nullable=True))
        batch_op.add_column(sa.Column("live_entry_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("live_exit_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("live_filled_amount", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("live_entry_fee", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("live_exit_fee", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("live_status", sa.String(length=40), nullable=True))
        batch_op.add_column(sa.Column("live_error", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("live_raw_open_response_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("live_raw_close_response_json", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.drop_column("live_raw_close_response_json")
        batch_op.drop_column("live_raw_open_response_json")
        batch_op.drop_column("live_error")
        batch_op.drop_column("live_status")
        batch_op.drop_column("live_exit_fee")
        batch_op.drop_column("live_entry_fee")
        batch_op.drop_column("live_filled_amount")
        batch_op.drop_column("live_exit_price")
        batch_op.drop_column("live_entry_price")
        batch_op.drop_column("live_close_order_id")
        batch_op.drop_column("live_exchange_order_id")
        batch_op.drop_column("execution_mode")

    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("live_reduce_only_close")
        batch_op.drop_column("live_order_type")
        batch_op.drop_column("live_max_total_loss_usdt")
        batch_op.drop_column("live_max_daily_loss_usdt")
        batch_op.drop_column("live_max_margin_usdt")
        batch_op.drop_column("live_kill_switch")
        batch_op.drop_column("live_enabled_confirmation")
        batch_op.drop_column("execution_mode")
