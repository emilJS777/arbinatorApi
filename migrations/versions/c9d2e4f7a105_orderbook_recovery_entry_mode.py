"""add order book recovery entry mode

Revision ID: c9d2e4f7a105
Revises: b8e4f1a9c203
Create Date: 2026-06-12
"""
from alembic import op
import sqlalchemy as sa


revision = "c9d2e4f7a105"
down_revision = "b8e4f1a9c203"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("entry_mode", sa.String(length=40), nullable=False, server_default="instant"))
        batch_op.add_column(sa.Column("confirmation_delay_seconds", sa.Float(), nullable=False, server_default="2"))
        batch_op.add_column(sa.Column("confirmation_max_wait_seconds", sa.Float(), nullable=False, server_default="5"))
        batch_op.add_column(sa.Column("confirmation_require_same_direction", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.add_column(sa.Column("confirmation_require_momentum_improvement", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.add_column(sa.Column("confirmation_min_momentum_delta", sa.Float(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("confirmation_require_consensus_still_valid", sa.Boolean(), nullable=False, server_default=sa.true()))

    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.add_column(sa.Column("entry_mode", sa.String(length=40), nullable=True, server_default="instant"))
        batch_op.add_column(sa.Column("first_signal_snapshot_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("confirmation_snapshot_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("confirmation_delay_actual_seconds", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("confirmation_result", sa.String(length=40), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.drop_column("confirmation_result")
        batch_op.drop_column("confirmation_delay_actual_seconds")
        batch_op.drop_column("confirmation_snapshot_json")
        batch_op.drop_column("first_signal_snapshot_json")
        batch_op.drop_column("entry_mode")

    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("confirmation_require_consensus_still_valid")
        batch_op.drop_column("confirmation_min_momentum_delta")
        batch_op.drop_column("confirmation_require_momentum_improvement")
        batch_op.drop_column("confirmation_require_same_direction")
        batch_op.drop_column("confirmation_max_wait_seconds")
        batch_op.drop_column("confirmation_delay_seconds")
        batch_op.drop_column("entry_mode")
