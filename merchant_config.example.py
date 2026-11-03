"""
merchant_config.example.py
===========================
Copy this file to merchant_config.py and fill in real values.
merchant_config.py is gitignored — thresholds are calibrated per merchant
and not included in this demo repo.

Each merchant entry shape:
  merchant_id            — data warehouse key
  name                   — display name for Slack messages
  enabled                — set False to pause monitoring without deleting config
  channel_id             — Slack channel for alerts
  mention_users_watch    — Slack user IDs tagged on WATCH alerts
  mention_users_critical — Slack user IDs tagged on CRITICAL alerts (usually adds escalation contacts)
  thresholds             — per-segment {min_attempts, sr_threshold}
                           min_attempts: ignore the segment unless volume exceeds this
                           sr_threshold: alert if SR drops below this %

Threshold calibration notes
----------------------------
Thresholds are set per merchant, not globally, because merchants have
different baseline SRs based on their card mix, user geography, and
payment method distribution. A single global threshold would produce
false positives for low-SR segments and miss genuine drops on high-SR ones.
"""

MERCHANTS = [
    {
        'merchant_id': 'YOUR_MERCHANT_ID_001',
        'name': 'Merchant Name Here',
        'enabled': True,
        'channel_id': 'C_YOUR_SLACK_CHANNEL_ID',
        'mention_users_watch':    ['U_SLACK_USER_1', 'U_SLACK_USER_2'],
        'mention_users_critical': ['U_SLACK_USER_1', 'U_SLACK_USER_2', 'U_ESCALATION_USER'],
        'thresholds': {
            'Overall': {
                'min_attempts': 0,      # set your minimum volume floor
                'sr_threshold': 0.0,    # set your SR floor (%)
            },
            'Segment Name 1': {
                'min_attempts': 0,
                'sr_threshold': 0.0,
            },
            # add more segments matching your payment taxonomy
        },
    },
    # add more merchants following the same shape
]
