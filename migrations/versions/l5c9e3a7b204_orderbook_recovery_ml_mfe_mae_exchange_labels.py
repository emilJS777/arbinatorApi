"""orderbook recovery ml mfe mae exchange labels

Revision ID: l5c9e3a7b204
Revises: k4b8c1d2e703
Create Date: 2026-06-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "l5c9e3a7b204"
down_revision = "k4b8c1d2e703"
branch_labels = None
depends_on = None


HORIZONS = (10, 30, 60)


def upgrade():
    with op.batch_alter_table("ml_market_snapshot") as batch_op:
        for horizon in HORIZONS:
            batch_op.add_column(sa.Column(f"max_price_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"min_price_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"mfe_long_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"mae_long_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"mfe_short_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"mae_short_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"median_future_price_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"median_future_return_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"avg_future_price_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"avg_future_return_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"median_mfe_long_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"median_mae_long_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"median_mfe_short_{horizon}s", sa.Float(), nullable=True))
            batch_op.add_column(sa.Column(f"median_mae_short_{horizon}s", sa.Float(), nullable=True))

    op.create_table(
        "ml_market_price_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("mid_price", sa.Float(), nullable=False),
        sa.Column("bid", sa.Float(), nullable=True),
        sa.Column("ask", sa.Float(), nullable=True),
        sa.Column("spread", sa.Float(), nullable=True),
        sa.Column("snapshot_age_sec", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ml_market_price_history_timestamp", "ml_market_price_history", ["timestamp"])
    op.create_index("ix_ml_market_price_history_exchange", "ml_market_price_history", ["exchange"])
    op.create_index("ix_ml_market_price_history_symbol", "ml_market_price_history", ["symbol"])

    exchange_label_columns = [
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("reference_price", sa.Float(), nullable=False),
    ]
    for horizon in HORIZONS:
        exchange_label_columns.extend([
            sa.Column(f"future_price_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"future_return_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"max_price_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"min_price_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"mfe_long_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"mae_long_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"mfe_short_{horizon}s", sa.Float(), nullable=True),
            sa.Column(f"mae_short_{horizon}s", sa.Float(), nullable=True),
        ])
    exchange_label_columns.extend([
        sa.Column("label_status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["snapshot_id"], ["ml_market_snapshot.id"]),
        sa.PrimaryKeyConstraint("id"),
    ])
    op.create_table("ml_market_snapshot_exchange_label", *exchange_label_columns)
    op.create_index("ix_ml_market_snapshot_exchange_label_snapshot_id", "ml_market_snapshot_exchange_label", ["snapshot_id"])
    op.create_index("ix_ml_market_snapshot_exchange_label_exchange", "ml_market_snapshot_exchange_label", ["exchange"])
    op.create_index("ix_ml_market_snapshot_exchange_label_symbol", "ml_market_snapshot_exchange_label", ["symbol"])
    op.create_index("ix_ml_market_snapshot_exchange_label_label_status", "ml_market_snapshot_exchange_label", ["label_status"])


def downgrade():
    op.drop_index("ix_ml_market_snapshot_exchange_label_label_status", table_name="ml_market_snapshot_exchange_label")
    op.drop_index("ix_ml_market_snapshot_exchange_label_symbol", table_name="ml_market_snapshot_exchange_label")
    op.drop_index("ix_ml_market_snapshot_exchange_label_exchange", table_name="ml_market_snapshot_exchange_label")
    op.drop_index("ix_ml_market_snapshot_exchange_label_snapshot_id", table_name="ml_market_snapshot_exchange_label")
    op.drop_table("ml_market_snapshot_exchange_label")

    op.drop_index("ix_ml_market_price_history_symbol", table_name="ml_market_price_history")
    op.drop_index("ix_ml_market_price_history_exchange", table_name="ml_market_price_history")
    op.drop_index("ix_ml_market_price_history_timestamp", table_name="ml_market_price_history")
    op.drop_table("ml_market_price_history")

    with op.batch_alter_table("ml_market_snapshot") as batch_op:
        for horizon in reversed(HORIZONS):
            batch_op.drop_column(f"median_mae_short_{horizon}s")
            batch_op.drop_column(f"median_mfe_short_{horizon}s")
            batch_op.drop_column(f"median_mae_long_{horizon}s")
            batch_op.drop_column(f"median_mfe_long_{horizon}s")
            batch_op.drop_column(f"avg_future_return_{horizon}s")
            batch_op.drop_column(f"avg_future_price_{horizon}s")
            batch_op.drop_column(f"median_future_return_{horizon}s")
            batch_op.drop_column(f"median_future_price_{horizon}s")
            batch_op.drop_column(f"mae_short_{horizon}s")
            batch_op.drop_column(f"mfe_short_{horizon}s")
            batch_op.drop_column(f"mae_long_{horizon}s")
            batch_op.drop_column(f"mfe_long_{horizon}s")
            batch_op.drop_column(f"min_price_{horizon}s")
            batch_op.drop_column(f"max_price_{horizon}s")
