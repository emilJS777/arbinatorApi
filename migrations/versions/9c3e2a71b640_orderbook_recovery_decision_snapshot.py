"""add order book recovery decision snapshot fields

Revision ID: 9c3e2a71b640
Revises: f2b8c6d1a904
Create Date: 2026-06-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "9c3e2a71b640"
down_revision = "f2b8c6d1a904"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("strategy_run_trade", schema=None) as batch_op:
        batch_op.add_column(sa.Column("decision_snapshot_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("per_exchange_features_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("consensus_direction", sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column("valid_exchanges_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("confirming_long_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("confirming_short_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("consensus_ratio_long", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("consensus_ratio_short", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("average_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("average_momentum", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("configured_exchange_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("configured_exchange_spread", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("configured_exchange_momentum", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("entry_reason", sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade", schema=None) as batch_op:
        batch_op.drop_column("entry_reason")
        batch_op.drop_column("configured_exchange_momentum")
        batch_op.drop_column("configured_exchange_spread")
        batch_op.drop_column("configured_exchange_imbalance")
        batch_op.drop_column("average_momentum")
        batch_op.drop_column("average_imbalance")
        batch_op.drop_column("consensus_ratio_short")
        batch_op.drop_column("consensus_ratio_long")
        batch_op.drop_column("confirming_short_count")
        batch_op.drop_column("confirming_long_count")
        batch_op.drop_column("valid_exchanges_count")
        batch_op.drop_column("consensus_direction")
        batch_op.drop_column("per_exchange_features_json")
        batch_op.drop_column("decision_snapshot_json")
