"""add live open failed cooldown

Revision ID: e2b6c9a4d801
Revises: d1a7c8e9f204
Create Date: 2026-06-12
"""
from alembic import op
import sqlalchemy as sa


revision = "e2b6c9a4d801"
down_revision = "d1a7c8e9f204"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("live_open_failed_cooldown_seconds", sa.Integer(), nullable=False, server_default="60"))


def downgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("live_open_failed_cooldown_seconds")
