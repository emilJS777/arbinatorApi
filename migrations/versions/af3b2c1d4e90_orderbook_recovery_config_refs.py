"""add order book recovery config exchange pair refs

Revision ID: af3b2c1d4e90
Revises: 9c3e2a71b640
Create Date: 2026-06-12
"""
from alembic import op
import sqlalchemy as sa


revision = "af3b2c1d4e90"
down_revision = "9c3e2a71b640"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("exchange_id", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("trading_pair_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_order_book_pattern_strategy_config_exchange_id",
            "exchange",
            ["exchange_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_order_book_pattern_strategy_config_trading_pair_id",
            "trading_pair",
            ["trading_pair_id"],
            ["id"],
        )


def downgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_constraint("fk_order_book_pattern_strategy_config_trading_pair_id", type_="foreignkey")
        batch_op.drop_constraint("fk_order_book_pattern_strategy_config_exchange_id", type_="foreignkey")
        batch_op.drop_column("trading_pair_id")
        batch_op.drop_column("exchange_id")
