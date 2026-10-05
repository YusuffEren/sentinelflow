# =============================================================================
# SentinelFlow - Fraud Detector CLI
# =============================================================================
"""Command line interface for the fraud detector service."""

from __future__ import annotations

import argparse
import sys

from loguru import logger

from sentinelflow.config import get_settings

from .service import FraudDetectorService


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="SentinelFlow Fraud Detector Service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--kafka-servers",
        "-k",
        type=str,
        default=None,
        help="Kafka bootstrap servers",
    )

    parser.add_argument(
        "--topic-in",
        "-i",
        type=str,
        default="transactions",
        help="Input topic for transactions",
    )

    parser.add_argument(
        "--topic-out",
        "-o",
        type=str,
        default="alerts",
        help="Output topic for fraud alerts",
    )

    parser.add_argument(
        "--consumer-group",
        "-g",
        type=str,
        default="sentinelflow-detectors",
        help="Kafka consumer group ID",
    )

    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Disable real-time dashboard",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    settings = get_settings()

    # Configure logging
    log_level = "DEBUG" if args.verbose else settings.log_level
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )

    # Initialize and start detector
    kafka_servers = args.kafka_servers or settings.kafka.bootstrap_servers

    detector = FraudDetectorService(
        kafka_servers=kafka_servers,
        kafka_topic_in=args.topic_in,
        kafka_topic_out=args.topic_out,
        consumer_group=args.consumer_group,
    )

    detector.start(show_dashboard=not args.no_dashboard)
