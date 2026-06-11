"""add order book recovery trade archive fields

Revision ID: e7a1c4d9b205
Revises: d4e9f2b6a103
Create Date: 2026-06-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "e7a1c4d9b205"
down_revision = "d4e9f2b6a103"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("strategy_run_trade", schema=None) as batch_op:
        batch_op.add_column(sa.Column("is_archived", sa.Boolean(), server_default=sa.false(), nullable=False))
        batch_op.add_column(sa.Column("archived_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("archive_reason", sa.String(length=120), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade", schema=None) as batch_op:
        batch_op.drop_column("archive_reason")
        batch_op.drop_column("archived_at")
        batch_op.drop_column("is_archived")
