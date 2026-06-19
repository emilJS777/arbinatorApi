"""orderbook recovery ml market snapshots

Revision ID: k4b8c1d2e703
Revises: j3f9a2c7b610
Create Date: 2026-06-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "k4b8c1d2e703"
down_revision = "j3f9a2c7b610"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("ml_snapshot_capture_enabled", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.add_column(sa.Column("ml_snapshot_sample_rate", sa.Float(), nullable=False, server_default="1.0"))
        batch_op.add_column(sa.Column("ml_label_horizons_seconds", sa.Text(), nullable=False, server_default="[10, 30, 60]"))
        batch_op.add_column(sa.Column("ml_max_snapshots_per_hour", sa.Integer(), nullable=False, server_default="10000"))

    op.create_table(
        "ml_market_snapshot",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("reference_price", sa.Float(), nullable=False),
        sa.Column("median_imbalance", sa.Float(), nullable=True),
        sa.Column("raw_avg_imbalance", sa.Float(), nullable=True),
        sa.Column("spread", sa.Float(), nullable=True),
        sa.Column("momentum", sa.Float(), nullable=True),
        sa.Column("valid_exchanges_count", sa.Integer(), nullable=True),
        sa.Column("long_confirms", sa.Integer(), nullable=True),
        sa.Column("short_confirms", sa.Integer(), nullable=True),
        sa.Column("long_ratio", sa.Float(), nullable=True),
        sa.Column("short_ratio", sa.Float(), nullable=True),
        sa.Column("anomaly_count", sa.Integer(), nullable=True),
        sa.Column("snapshot_age_sec", sa.Float(), nullable=True),
        sa.Column("configured_exchange_long_signal", sa.Boolean(), nullable=True),
        sa.Column("configured_exchange_short_signal", sa.Boolean(), nullable=True),
        sa.Column("proposed_side", sa.String(length=20), nullable=True),
        sa.Column("final_side", sa.String(length=20), nullable=True),
        sa.Column("reject_reason", sa.String(length=120), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("future_price_10s", sa.Float(), nullable=True),
        sa.Column("future_return_10s", sa.Float(), nullable=True),
        sa.Column("long_would_win_10s", sa.Boolean(), nullable=True),
        sa.Column("short_would_win_10s", sa.Boolean(), nullable=True),
        sa.Column("future_price_30s", sa.Float(), nullable=True),
        sa.Column("future_return_30s", sa.Float(), nullable=True),
        sa.Column("long_would_win_30s", sa.Boolean(), nullable=True),
        sa.Column("short_would_win_30s", sa.Boolean(), nullable=True),
        sa.Column("future_price_60s", sa.Float(), nullable=True),
        sa.Column("future_return_60s", sa.Float(), nullable=True),
        sa.Column("long_would_win_60s", sa.Boolean(), nullable=True),
        sa.Column("short_would_win_60s", sa.Boolean(), nullable=True),
        sa.Column("label_status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ml_market_snapshot_timestamp", "ml_market_snapshot", ["timestamp"])
    op.create_index("ix_ml_market_snapshot_symbol", "ml_market_snapshot", ["symbol"])
    op.create_index("ix_ml_market_snapshot_exchange", "ml_market_snapshot", ["exchange"])
    op.create_index("ix_ml_market_snapshot_label_status", "ml_market_snapshot", ["label_status"])


def downgrade():
    op.drop_index("ix_ml_market_snapshot_label_status", table_name="ml_market_snapshot")
    op.drop_index("ix_ml_market_snapshot_exchange", table_name="ml_market_snapshot")
    op.drop_index("ix_ml_market_snapshot_symbol", table_name="ml_market_snapshot")
    op.drop_index("ix_ml_market_snapshot_timestamp", table_name="ml_market_snapshot")
    op.drop_table("ml_market_snapshot")
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("ml_max_snapshots_per_hour")
        batch_op.drop_column("ml_label_horizons_seconds")
        batch_op.drop_column("ml_snapshot_sample_rate")
        batch_op.drop_column("ml_snapshot_capture_enabled")
