"""Opt-in versioned paper experiment; existing baseline values are unchanged."""
from alembic import op
import sqlalchemy as sa

revision = "q0b5c8e2f709"
down_revision = "p9a4b7d1e608"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("order_book_pattern_strategy_config", sa.Column("strategy_version", sa.String(40), nullable=False, server_default="baseline"))
    op.add_column("order_book_pattern_strategy_config", sa.Column("experiment_settings", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("order_book_pattern_strategy_config", "experiment_settings")
    op.drop_column("order_book_pattern_strategy_config", "strategy_version")
