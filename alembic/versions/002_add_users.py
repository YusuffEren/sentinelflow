"""Add users and refresh_tokens tables

Revision ID: 002
Revises: 001
Create Date: 2026-03-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ==========================================================================
    # Users table
    # ==========================================================================
    op.create_table(
        'users',
        sa.Column('user_id', sa.String(50), primary_key=True),
        
        # Credentials
        sa.Column('username', sa.String(50), unique=True, nullable=False, index=True),
        sa.Column('email', sa.String(255), unique=True, nullable=False, index=True),
        sa.Column('password_hash', sa.String(255), nullable=False),
        
        # Profile
        sa.Column('full_name', sa.String(200), nullable=False),
        sa.Column('role', sa.String(20), default='viewer', index=True),
        sa.Column('status', sa.String(20), default='active', index=True),
        sa.Column('team', sa.String(100), nullable=True),
        
        # Timestamps
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.Column('last_login', sa.DateTime(timezone=True), nullable=True),
        
        # Settings
        sa.Column('preferences', postgresql.JSONB(), default={}),
        
        # Security
        sa.Column('failed_login_attempts', sa.Integer(), default=0),
        sa.Column('locked_until', sa.DateTime(timezone=True), nullable=True),
    )
    
    # ==========================================================================
    # Refresh Tokens table
    # ==========================================================================
    op.create_table(
        'refresh_tokens',
        sa.Column('token_id', sa.String(50), primary_key=True),
        sa.Column('user_id', sa.String(50), sa.ForeignKey('users.user_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('token_hash', sa.String(255), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('user_agent', sa.Text(), nullable=True),
        sa.Column('ip_address', sa.String(45), nullable=True),
        sa.Column('is_revoked', sa.Boolean(), default=False),
    )
    
    # ==========================================================================
    # Create default admin user
    # Password: Admin123! (hashed with bcrypt)
    # ==========================================================================
    op.execute("""
        INSERT INTO users (user_id, username, email, password_hash, full_name, role, status)
        VALUES (
            'USR-ADMIN000001',
            'admin',
            'admin@sentinelflow.dev',
            '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4.GQzPJLvCXpRqGK',
            'System Administrator',
            'admin',
            'active'
        )
        ON CONFLICT (username) DO NOTHING;
    """)


def downgrade() -> None:
    op.drop_table('refresh_tokens')
    op.drop_table('users')
