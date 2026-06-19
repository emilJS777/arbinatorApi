"""orderbook recovery ml shadow mode

Revision ID: j3f9a2c7b610
Revises: i2d4e6f8a901
Create Date: 2026-06-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "j3f9a2c7b610"
down_revision = "i2d4e6f8a901"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("ml_mode", sa.String(length=20), nullable=False, server_default="disabled"))

    op.create_table(
        "ml_feature_snapshot",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("trade_id", sa.Integer(), nullable=True),
        sa.Column("evaluation_id", sa.String(length=80), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("proposed_side", sa.String(length=20), nullable=True),
        sa.Column("final_side", sa.String(length=20), nullable=True),
        sa.Column("median_imbalance", sa.Float(), nullable=True),
        sa.Column("raw_avg_imbalance", sa.Float(), nullable=True),
        sa.Column("spread", sa.Float(), nullable=True),
        sa.Column("momentum", sa.Float(), nullable=True),
        sa.Column("valid_exchanges_count", sa.Integer(), nullable=True),
        sa.Column("confirming_long_count", sa.Integer(), nullable=True),
        sa.Column("confirming_short_count", sa.Integer(), nullable=True),
        sa.Column("consensus_ratio_long", sa.Float(), nullable=True),
        sa.Column("consensus_ratio_short", sa.Float(), nullable=True),
        sa.Column("anomaly_count", sa.Integer(), nullable=True),
        sa.Column("snapshot_age_sec", sa.Float(), nullable=True),
        sa.Column("configured_exchange_long_signal", sa.Boolean(), nullable=True),
        sa.Column("configured_exchange_short_signal", sa.Boolean(), nullable=True),
        sa.Column("current_step", sa.Integer(), nullable=True),
        sa.Column("margin", sa.Float(), nullable=True),
        sa.Column("leverage", sa.Float(), nullable=True),
        sa.Column("tp_percent", sa.Float(), nullable=True),
        sa.Column("sl_percent", sa.Float(), nullable=True),
        sa.Column("entry_mode", sa.String(length=40), nullable=True),
        sa.Column("result", sa.String(length=20), nullable=True),
        sa.Column("gross_pnl", sa.Float(), nullable=True),
        sa.Column("net_pnl", sa.Float(), nullable=True),
        sa.Column("total_fee", sa.Float(), nullable=True),
        sa.Column("ml_score", sa.Float(), nullable=True),
        sa.Column("ml_decision", sa.String(length=40), nullable=True),
        sa.Column("ml_reason", sa.String(length=255), nullable=True),
        sa.Column("ml_model_version", sa.String(length=80), nullable=True),
        sa.ForeignKeyConstraint(["trade_id"], ["strategy_run_trade.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ml_feature_snapshot_evaluation_id", "ml_feature_snapshot", ["evaluation_id"])
    op.create_index("ix_ml_feature_snapshot_timestamp", "ml_feature_snapshot", ["timestamp"])


def downgrade():
    op.drop_index("ix_ml_feature_snapshot_timestamp", table_name="ml_feature_snapshot")
    op.drop_index("ix_ml_feature_snapshot_evaluation_id", table_name="ml_feature_snapshot")
    op.drop_table("ml_feature_snapshot")
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("ml_mode")
