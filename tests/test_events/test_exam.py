import random
import pytest

from datetime import datetime, timedelta
from synthetic.conf import CatalogConfig, EngagementConfig, ProfileConfig, global_conf, EventConfig
from synthetic.catalog.cache import CatalogCache
from synthetic.constants import CatalogType
from synthetic.event.constants import EventType
from synthetic.event.log.general.rate import RateEvent
from synthetic.event.log.learning.exam import ExamEvent
from synthetic.event.log.learning.question import QuestionEvent, QuestionAction
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "exam_guy": ProfileConfig(
            session_engagement=EngagementConfig(initial_min=1.0, initial_max=1.0, change_probability=0.0),
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            session_event_type_changes_min=0,
            session_event_type_changes_max=0,
            session_length_min_seconds=1000,
            session_length_max_seconds=1000,
            online_probability=0.5,
            event_probabilities={EventType.EXAM: 1.0},
            events={
                EventType.EXAM: EventConfig(
                    properties={"abort_probability": 0.0, "skip_probability": 0.0, "pass_probability": 1.0}
                )
            },
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def exam_user(driver_meta, registration_ts) -> SessionEngagementUser:
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="exam_guy")


def test_single_exam_completed(db_session, driver_meta, registration_ts, exam_user):
    global_conf.catalogs[CatalogType.EXAM] = CatalogConfig(
        target_count=1,
        properties={"question_count_min": 5, "question_count_max": 5, "difficulty_min": 0.0, "difficulty_max": 0.0},
    )
    global_conf.rating_probability = 1.0

    random.seed(0)

    CatalogCache.warm_up(db_session, driver_meta_id=driver_meta.id)

    assert len(CatalogCache.cached_catalog[CatalogType.EXAM]) == 1

    exam_meta = CatalogCache.get_random_catalog_of_type(CatalogType.EXAM)
    question_metas = CatalogCache.get_catalogs_by_properties(CatalogType.QUESTION, {"exam_uuid": exam_meta["uuid"]})

    end_ts = registration_ts + timedelta(days=1)
    events = exam_user.generate_events(end_ts)

    log_events = events.log_events
    question_events = [event for event in log_events if isinstance(event, QuestionEvent)]
    exam_events = sorted([event for event in log_events if isinstance(event, ExamEvent)], key=lambda event: event.ts)

    assert len(exam_events) == 3
    assert_dicts_equal_partial(exam_events[0].props, {"action": "start", "id": exam_meta["uuid"]})
    assert_dicts_equal_partial(
        exam_events[1].props,
        {
            "action": "submit",
            "id": exam_meta["uuid"],
        },
    )
    assert exam_events[1].props["duration"] > 0

    assert_dicts_equal_partial(exam_events[2].props, {"action": "result", "id": exam_meta["uuid"]})
    assert exam_events[2].props["score"] == 100.0
    assert exam_events[2].props["is_passed"] is True

    assert len(question_events) == 5
    question_events_by_uuid = dict([(event.props["id"], event) for event in question_events])

    for question_meta in question_metas:
        question_event = question_events_by_uuid[question_meta["uuid"]]
        assert_dicts_equal_partial(
            question_event.props,
            {
                "exam_id": exam_meta["uuid"],
                "action": QuestionAction.ANSWER.value,
                "answer_id": question_meta["correct_answer_uuid"],
            },
        )

    rate_events = [event for event in log_events if isinstance(event, RateEvent)]
    assert len(rate_events) > 0

    assert_events_have_correct_schema(events)
