`active-alerts-20.json` is derived from a read-only Alertmanager capture on
2026-09-12: 19 TailscalePathDegraded alerts and one SystemdUnitFailed alert.
Host/peer/unit names, fingerprints, routing labels, descriptions, receiver names
and graph URLs are anonymized. The alert count, timestamps, distinct host/peer
relationships and summary template structure are preserved. This is an offline
regression fixture; passing it does not assert production rollout or fleet health.
