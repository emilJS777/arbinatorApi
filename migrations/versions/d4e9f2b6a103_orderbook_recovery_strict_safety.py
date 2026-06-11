"""add order book recovery strict safety settings

Revision ID: d4e9f2b6a103
Revises: c42f8a91d5b0
Create Date: 2026-06-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "d4e9f2b6a103"
down_revision = "c42f8a91d5b0"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config", schema=None) as batch_op:
        batch_op.add_column(sa.Column("min_valid_exchanges", sa.Integer(), server_default="2", nullable=False))
        batch_op.add_column(sa.Column("cooldown_after_max_recovery_seconds", sa.Integer(), server_default="600", nullable=False))

    with op.batch_alter_table("recovery_state", schema=None) as batch_op:
        batch_op.add_column(sa.Column("paused_until", sa.DateTime(), nullable=True))


def downgrade():
    with op.batch_alter_table("recovery_state", schema=None) as batch_op:
        batch_op.drop_column("paused_until")

    with op.batch_alter_table("order_book_pattern_strategy_config", schema=None) as batch_op:
        batch_op.drop_column("cooldown_after_max_recovery_seconds")
        batch_op.drop_column("min_valid_exchanges")
