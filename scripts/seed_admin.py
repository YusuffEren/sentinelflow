# =============================================================================
# SentinelFlow - Seed Admin User
# =============================================================================
"""
Creates the initial admin user for the platform.

Usage:
    python scripts/seed_admin.py

Credentials are read from environment variables (or .env):
    SEED_ADMIN_USERNAME  (default: admin)
    SEED_ADMIN_EMAIL     (default: admin@sentinelflow.local)
    SEED_ADMIN_PASSWORD  (default: Admin123!)
    SEED_ADMIN_FULL_NAME (default: SentinelFlow Admin)
"""

from __future__ import annotations

import os
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from loguru import logger

from sentinelflow.auth.service import AuthService
from sentinelflow.contracts import UserCreate, UserRole
from sentinelflow.database.postgres import DatabaseSession, init_db


def main() -> int:
    username = os.getenv("SEED_ADMIN_USERNAME", "admin")
    email = os.getenv("SEED_ADMIN_EMAIL", "admin@sentinelflow.local")
    password = os.getenv("SEED_ADMIN_PASSWORD", "Admin123!")
    full_name = os.getenv("SEED_ADMIN_FULL_NAME", "SentinelFlow Admin")

    init_db()

    with DatabaseSession() as session:
        auth_service = AuthService(session)

        if auth_service._get_user_by_username(username):
            logger.info(f"User '{username}' already exists, skipping seed")
            return 0

        user_data = UserCreate(
            username=username,
            email=email,
            password=password,
            full_name=full_name,
            role=UserRole.ADMIN,
        )

        user = auth_service.register(user_data)
        logger.info(f"Admin user created: {user.username} ({user.user_id})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
