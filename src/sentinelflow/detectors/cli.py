# =============================================================================
# SentinelFlow - Detection CLI
# =============================================================================
"""
CLI entry points for SentinelFlow fraud detection services.

Delegates to the main processor modules.
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger


def run_detector(args: argparse.Namespace) -> None:
    """Run the main fraud detector service."""
    from sentinelflow.processor.detector import main as detector_main

    sys.argv = [sys.argv[0]]
    if args.kafka_servers:
        sys.argv.extend(["--kafka-servers", args.kafka_servers])
    if args.topic:
        sys.argv.extend(["--topic", args.topic])
    if args.group:
        sys.argv.extend(["--group", args.group])
    detector_main()


def run_graph(args: argparse.Namespace) -> None:
    """Run the Neo4j graph engine standalone."""
    from sentinelflow.processor.graph_engine import main as graph_main  # type: ignore

    graph_main()


def run_geo(args: argparse.Namespace) -> None:
    """Run the Redis geo-detection standalone."""
    from sentinelflow.processor.redis_geo import main as geo_main  # type: ignore

    geo_main()


def main() -> None:
    """Main CLI entry point for detection commands."""
    parser = argparse.ArgumentParser(
        description="SentinelFlow - Fraud Detection CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m sentinelflow.detectors run
  python -m sentinelflow.detectors run --kafka-servers localhost:9092
  python -m sentinelflow.detectors graph
  python -m sentinelflow.detectors geo
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Detection command")

    # Run detector
    run_parser = subparsers.add_parser("run", help="Run the main fraud detector")
    run_parser.add_argument(
        "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    run_parser.add_argument("--topic", default="transactions", help="Kafka topic")
    run_parser.add_argument("--group", default="sentinelflow-consumers", help="Consumer group")
    run_parser.set_defaults(func=run_detector)

    # Run graph engine
    graph_parser = subparsers.add_parser("graph", help="Run Neo4j graph engine")
    graph_parser.set_defaults(func=run_graph)

    # Run geo detection
    geo_parser = subparsers.add_parser("geo", help="Run Redis geo-detection")
    geo_parser.set_defaults(func=run_geo)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    logger.info(f"Starting detector command: {args.command}")
    args.func(args)


if __name__ == "__main__":
    main()
