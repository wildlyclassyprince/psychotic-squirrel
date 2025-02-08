from dotenv import dotenv_values
from pathlib import Path

# Environment variables
IHC_API_TOKEN = dotenv_values(Path(".env"))["IHC_API_TOKEN"]

# Constants
DB_NAME = "data/db/challenge.db"
SCHEMAS = {
    "attribution_customer_journey": "attribution_customer_journey.sql",
    "channel_reporting": "channel_reporting.sql",
    "session_costs": "session_costs.sql",
}
INGESTION = {
    "customer_journeys": "customer_journeys.sql",
}
REDISTRIBUTION_PARAMETER = {
    "initializer": {
        "direction": "earlier_sessions_only",
        "receive_threshold": 0,
        "redistribution_channel_labels": [
            "Affiliate & Partnerships",
            "Direct Traffic",
            "FB & IG Ads",
            "Lead Generation",
            "Microsoft Ads",
            "Newsletter & Email",
            "Organic Traffic",
            "Paid Search Brand",
            "Paid Search Non Brand",
            "Performance Max",
            "Pinterest Ads",
            "Referral",
            "Social Organic",
            "TikTok Ads",
            "Untracked Conversions",
            "YouTube Ads",
        ],
    },
    "holder": {
        "direction": "any_session",
        "receive_threshold": 0,
        "redistribution_channel_labels": [
            "Affiliate & Partnerships",
            "Direct Traffic",
            "FB & IG Ads",
            "Lead Generation",
            "Microsoft Ads",
            "Newsletter & Email",
            "Organic Traffic",
            "Paid Search Brand",
            "Paid Search Non Brand",
            "Performance Max",
            "Pinterest Ads",
            "Referral",
            "Social Organic",
            "TikTok Ads",
            "Untracked Conversions",
            "YouTube Ads",
        ],
    },
    "closer": {
        "direction": "later_sessions_only",
        "receive_threshold": 0.1,
        "redistribution_channel_labels": [
            "Paid Search Brand",
            "Newsletter & Email",
            "Performance Max",
        ],
    },
}
CONV_TYPE_ID = "test_attribution"
CHUNK_SIZE = 100
