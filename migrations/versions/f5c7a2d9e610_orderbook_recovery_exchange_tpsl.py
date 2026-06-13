"""add exchange side tpsl fields

Revision ID: f5c7a2d9e610
Revises: e2b6c9a4d801
Create Date: 2026-06-13
"""
from alembic import op
import sqlalchemy as sa


revision = "f5c7a2d9e610"
down_revision = "e2b6c9a4d801"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.add_column(sa.Column("exchange_tp_order_id", sa.String(length=120), nullable=True))
        batch_op.add_column(sa.Column("exchange_sl_order_id", sa.String(length=120), nullable=True))
        batch_op.add_column(sa.Column("exchange_tp_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("exchange_sl_price", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("tp_sl_protected", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch_op.add_column(sa.Column("tp_sl_error", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("tp_sl_created_at", sa.DateTime(), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.drop_column("tp_sl_created_at")
        batch_op.drop_column("tp_sl_error")
        batch_op.drop_column("tp_sl_protected")
        batch_op.drop_column("exchange_sl_price")
        batch_op.drop_column("exchange_tp_price")
        batch_op.drop_column("exchange_sl_order_id")
        batch_op.drop_column("exchange_tp_order_id")
