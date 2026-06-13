"""add live reconciliation fields

Revision ID: f7a4d8c2e901
Revises: f5c7a2d9e610
Create Date: 2026-06-13
"""
from alembic import op
import sqlalchemy as sa


revision = "f7a4d8c2e901"
down_revision = "f5c7a2d9e610"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.add_column(sa.Column("exit_price_fallback_used", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch_op.add_column(sa.Column("exit_price_warning", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("pnl_source", sa.String(length=40), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.drop_column("pnl_source")
        batch_op.drop_column("exit_price_warning")
        batch_op.drop_column("exit_price_fallback_used")
