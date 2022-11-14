from enum import Enum


class SyntheticUserType(Enum):
    # Session engagement management and a linear distribution of events
    SESSION_ENGAGEMENT = "session_engagement"

    # Both engagement and purchase engagement management for e-commerce
    PURCHASE_ENGAGEMENT = "purchase_engagement"

    # A simple user that visits one page every second
    EVENT_PER_PERIOD = "event_per_period"
