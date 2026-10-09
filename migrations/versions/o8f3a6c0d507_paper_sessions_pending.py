"""Paper experiment boundaries and durable pending lifecycle; no history reset."""
from alembic import op
import sqlalchemy as sa

revision = "o8f3a6c0d507"
down_revision = "n7e2f5b9c406"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("orderbook_paper_session",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("strategy_config_id", sa.Integer(), nullable=False),
        sa.Column("initial_equity_usdt", sa.Float(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("ended_at", sa.DateTime(), nullable=True))
    op.create_index("ix_orderbook_paper_session_strategy_config_id", "orderbook_paper_session", ["strategy_config_id"])
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch:
        batch.add_column(sa.Column("pending_entry_ttl_seconds", sa.Float(), nullable=False, server_default="5"))
        batch.add_column(sa.Column("paper_session_id", sa.String(36), nullable=True))
        batch.create_foreign_key("fk_config_paper_session", "orderbook_paper_session", ["paper_session_id"], ["id"])
    with op.batch_alter_table("strategy_run_trade") as batch:
        batch.add_column(sa.Column("paper_session_id", sa.String(36), nullable=True))
        batch.add_column(sa.Column("pending_entry_expires_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("paper_exit_status", sa.String(100), nullable=True))
        batch.create_foreign_key("fk_trade_paper_session", "orderbook_paper_session", ["paper_session_id"], ["id"])
        batch.create_index("ix_strategy_run_trade_paper_session_id", ["paper_session_id"])


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch:
        batch.drop_constraint("fk_trade_paper_session", type_="foreignkey")
        batch.drop_index("ix_strategy_run_trade_paper_session_id")
        for name in ("paper_exit_status", "pending_entry_expires_at", "paper_session_id"):
            batch.drop_column(name)
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch:
        batch.drop_constraint("fk_config_paper_session", type_="foreignkey")
        batch.drop_column("paper_session_id")
        batch.drop_column("pending_entry_ttl_seconds")
    op.drop_table("orderbook_paper_session")
