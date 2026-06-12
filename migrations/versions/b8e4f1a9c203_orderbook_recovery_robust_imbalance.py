"""add robust imbalance consensus settings

Revision ID: b8e4f1a9c203
Revises: af3b2c1d4e90
Create Date: 2026-06-12
"""
from alembic import op
import sqlalchemy as sa


revision = "b8e4f1a9c203"
down_revision = "af3b2c1d4e90"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("use_median_imbalance", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.add_column(sa.Column("imbalance_anomaly_min", sa.Float(), nullable=False, server_default="0.1"))
        batch_op.add_column(sa.Column("imbalance_anomaly_max", sa.Float(), nullable=False, server_default="10"))
        batch_op.add_column(sa.Column("exclude_anomalous_imbalance", sa.Boolean(), nullable=False, server_default=sa.true()))

    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.add_column(sa.Column("signal_median_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_raw_average_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_anomalous_exchanges_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("signal_excluded_anomalous_imbalance_exchanges_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("median_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("raw_average_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("anomalous_exchanges_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("excluded_anomalous_imbalance_exchanges_json", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.drop_column("excluded_anomalous_imbalance_exchanges_json")
        batch_op.drop_column("anomalous_exchanges_count")
        batch_op.drop_column("raw_average_imbalance")
        batch_op.drop_column("median_imbalance")
        batch_op.drop_column("signal_excluded_anomalous_imbalance_exchanges_json")
        batch_op.drop_column("signal_anomalous_exchanges_count")
        batch_op.drop_column("signal_raw_average_imbalance")
        batch_op.drop_column("signal_median_imbalance")

    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("exclude_anomalous_imbalance")
        batch_op.drop_column("imbalance_anomaly_max")
        batch_op.drop_column("imbalance_anomaly_min")
        batch_op.drop_column("use_median_imbalance")
