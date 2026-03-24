"""
GCP Pub/Sub publisher for reporium-ingestion.

Publishes a `repo-ingested` event after each successful batch upsert.
Falls back silently if google-cloud-pubsub is not installed or the topic is not configured.
"""
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def publish_repo_ingested(
    *,
    run_mode: str,
    upserted: int,
    repo_names: list[str],
    topic: str | None = None,
) -> None:
    """
    Publish a repo-ingested event to GCP Pub/Sub.

    Args:
        run_mode: The ingestion mode that just completed (quick/weekly/full/fix).
        upserted: Number of repos successfully upserted.
        repo_names: List of repo names that were updated.
        topic: Override topic path. Falls back to PUBSUB_REPO_INGESTED_TOPIC env var.
    """
    topic = topic or os.getenv("PUBSUB_REPO_INGESTED_TOPIC", "")
    if not topic:
        logger.debug("PUBSUB_REPO_INGESTED_TOPIC not set — skipping event publish")
        return

    try:
        from google.cloud import pubsub_v1  # type: ignore

        publisher = pubsub_v1.PublisherClient()
        payload = json.dumps(
            {
                "event": "repo.ingested",
                "run_mode": run_mode,
                "upserted": upserted,
                "repo_count": len(repo_names),
                "repo_names": repo_names[:200],  # cap to avoid oversized messages
                "published_at": datetime.now(timezone.utc).isoformat(),
            }
        ).encode("utf-8")

        future = publisher.publish(topic, payload)
        message_id = future.result(timeout=10)
        logger.info("Published repo-ingested event: message_id=%s upserted=%d", message_id, upserted)

    except ImportError:
        logger.debug("google-cloud-pubsub not installed — skipping event publish")
    except Exception as exc:
        # Non-critical: never crash the ingestion run over a publish failure
        logger.warning("Failed to publish repo-ingested event: %s", exc)
