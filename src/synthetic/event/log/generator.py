import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Sequence, Tuple
from uuid import uuid4

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import global_conf
from synthetic.constants import CatalogType, BlockType
from synthetic.event.constants import EventType, MediaType
from synthetic.event.log.commerce.constants import ItemObject, ItemType
from synthetic.event.log.commerce.item import ItemEvent, ItemAction
from synthetic.event.log.general.rate import RateEvent
from synthetic.event.log.loyalty.level import LevelEvent
from synthetic.event.log.loyalty.milestone import MilestoneEvent, MilestoneAction
from synthetic.event.log.learning.exam import ExamEvent, ExamAction
from synthetic.event.log.learning.module import ModuleEvent, ModuleAction
from synthetic.event.log.learning.question import QuestionEvent, QuestionAction
from synthetic.event.log.log_base import LogEvent
from synthetic.event.log.general.media import (
    MediaEvent,
    MediaAction,
)
from synthetic.event.log.general.page import PageEvent
from synthetic.event.log.general.search import SearchEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.random import get_random_int_in_range, get_random_float_in_range, generate_random_rate_value
from synthetic.utils.time_utils import total_difference_seconds
from synthetic.utils.user_utils import fake

logger = logging.getLogger(__name__)


def generate_rate_events(
    user: SyntheticUser, current_ts: datetime, subject_id: str, catalog_type: CatalogType
) -> Tuple[List[RateEvent], datetime]:
    rate_events = []
    rating_probability = global_conf.rating_probability
    if rating_probability > 0.0 and random.random() < rating_probability:
        current_ts += timedelta(seconds=random.randint(5, 30))
        rate_events.append(
            RateEvent(
                user,
                current_ts,
                True,
                subject_id,
                catalog_type,
                generate_random_rate_value(),
            )
        )
    return rate_events, current_ts


def generate_event_logs_of_type(
    synthetic_user: SyntheticUser,
    current_session_ts: datetime,
    event_type: EventType,
    online: bool = None,
) -> Tuple[Sequence[LogEvent], datetime]:
    profile_config = synthetic_user.get_profile_conf()

    if online is None:
        online = random.random() < profile_config.online_probability

    events: Sequence[LogEvent]
    generation_start_ts = current_session_ts

    if event_type == EventType.PAGE:
        event_conf = profile_config.get_event_config(event_type)
        min_page_count = event_conf.properties.get("page_count_per_session_min", 1)
        max_page_count = event_conf.properties.get("page_count_per_session_min", 5)

        events, current_session_ts = generate_page_sequence(
            synthetic_user,
            current_session_ts,
            online,
            event_count=get_random_int_in_range(min_page_count, max_page_count),
        )
    elif event_type == EventType.SEARCH:
        event_conf = profile_config.get_event_config(event_type)
        min_page_count = event_conf.properties.get("page_count_per_session_min", 1)
        max_page_count = event_conf.properties.get("page_count_per_session_min", 5)

        events, current_session_ts = generate_search_sequence(
            synthetic_user,
            current_session_ts,
            online,
            page_count=get_random_int_in_range(min_page_count, max_page_count),
        )
    elif event_type == EventType.VIDEO:
        video_meta = CatalogCache.get_random_catalog_of_type(CatalogType.MEDIA_VIDEO)
        events, current_session_ts = generate_media_sequence(synthetic_user, current_session_ts, online, video_meta)
    elif event_type == EventType.AUDIO:
        audio_meta = CatalogCache.get_random_catalog_of_type(CatalogType.MEDIA_AUDIO)
        events, current_session_ts = generate_media_sequence(synthetic_user, current_session_ts, online, audio_meta)
    elif event_type == EventType.IMAGE:
        image_meta = CatalogCache.get_random_catalog_of_type(CatalogType.MEDIA_IMAGE)
        events, current_session_ts = generate_media_sequence(synthetic_user, current_session_ts, online, image_meta)
    elif event_type == EventType.MODULE:
        events, current_session_ts = generate_module_events(synthetic_user, current_session_ts, online)
    elif event_type == EventType.EXAM:
        events, current_session_ts = generate_exam_events(synthetic_user, current_session_ts, online)
    else:
        raise ValueError("Unsupported event type: %s" % (event_type,))

    assert len(events) > 0
    assert current_session_ts > generation_start_ts, f"{event_type} did not increment time!"

    return events, current_session_ts


def generate_media_sequence(
    user: SyntheticUser, ts: datetime, online: bool, media_meta: Dict
) -> Tuple[Sequence[LogEvent], datetime]:
    events: List[LogEvent] = []
    media_uuid = media_meta["uuid"]
    media_type = media_meta["media_type"]

    event_config = user.get_profile_conf().get_event_config(EventType(media_type.value))
    duration_seconds_min = event_config.properties.get("duration_seconds_min", 30)
    duration_seconds_max = event_config.properties.get("duration_seconds_max", 60)

    events.append(MediaEvent(user, ts, online, media_type, media_uuid, MediaAction.IMPRESSION, 0))
    ts += timedelta(seconds=get_random_float_in_range(0.5, 5))

    if media_type == MediaType.IMAGE:
        events.append(MediaEvent(user, ts, online, media_type, media_uuid, MediaAction.PLAY, 0))
        ts += timedelta(seconds=get_random_int_in_range(duration_seconds_min, duration_seconds_max))
    else:
        start_ts = ts
        media_length_ms = media_meta["length"]
        events.append(MediaEvent(user, ts, online, media_type, media_uuid, MediaAction.PLAY, 0))
        event_duration = get_random_int_in_range(duration_seconds_min, duration_seconds_max)
        ts += timedelta(seconds=event_duration)

        pause_probability = event_config.properties.get("pause_probability", 0.3)
        logger.debug("Generating media with pause probability: %s", pause_probability)
        if random.random() <= pause_probability:
            view_ratio = random.random() * 0.6
            pause_duration = event_duration * view_ratio
            events.append(
                MediaEvent(
                    user,
                    start_ts + timedelta(seconds=pause_duration),
                    online,
                    media_type,
                    media_uuid,
                    MediaAction.PAUSE,
                    round(media_length_ms * view_ratio),
                )
            )

        events.append(MediaEvent(user, ts, online, media_type, media_uuid, MediaAction.FINISH, media_length_ms))

    rate_events, ts = generate_rate_events(user, ts, media_uuid, CatalogType[f"MEDIA_{media_type.value.upper()}"])
    if len(rate_events) > 0:
        events.extend(rate_events)

    return events, ts


def generate_milestone_and_level_events(
    user: SyntheticUser, current_ts: datetime, online: bool, score_gained: float, block: BlockType
) -> Tuple[List[LogEvent], datetime]:
    mil_lev_events: List[LogEvent] = []
    milestone_metas = CatalogCache.get_all_catalogs(CatalogType.MILESTONE)

    achieved_milestone_uuids = set(user.milestone_achieved_uuids)
    user.set_level_score(block, user.get_level_score(block) + score_gained, current_ts)
    current_level_score = user.get_level_score(block)
    for milestone_meta in milestone_metas:
        if milestone_meta["uuid"] in achieved_milestone_uuids:
            continue

        if milestone_meta["required_score"] <= current_level_score:
            mil_lev_events.append(
                MilestoneEvent(user, current_ts, online, milestone_meta["uuid"], MilestoneAction.ACHIEVED)
            )
            user.set_milestone_achieved(milestone_meta["uuid"], current_ts=current_ts)

            user.set_current_level(user.level + 1, current_ts=current_ts)
            mil_lev_events.append(LevelEvent(user, current_ts, online, prev_level=user.level - 1, new_level=user.level))
            current_ts += timedelta(seconds=get_random_int_in_range(5, 30))

    return mil_lev_events, current_ts


def generate_module_events(user: SyntheticUser, ts: datetime, online: bool) -> Tuple[Sequence[LogEvent], datetime]:
    events: List[LogEvent] = []

    active_modules_uuids = user.get_active_module_uuids()
    user_profile_conf = user.get_profile_conf()

    min_duration = user_profile_conf.session_length_min_seconds
    max_duration = user_profile_conf.session_length_max_seconds
    session_duration = random.randrange(min_duration, max_duration) if max_duration > min_duration else min_duration

    logger.debug("Generating module events for a session of %s seconds...", session_duration)
    if len(active_modules_uuids) > 0:
        # Continue / finish a random module
        module_meta = CatalogCache.get_catalog_by_uuid(CatalogType.MODULE, random.choice(active_modules_uuids))
    else:
        # Start a random module
        module_meta = CatalogCache.get_random_catalog_of_type(CatalogType.MODULE)

        logger.debug(
            "%s starting new module: %s, duration %s",
            user.get_platform_uuid(),
            module_meta["uuid"],
            module_meta["duration"],
        )
        user.start_module(module_meta["uuid"], module_meta["duration"])

        events.append(ModuleEvent(user, ts, online, module_meta["uuid"], ModuleAction.VIEW, 0))
        ts += timedelta(seconds=random.randrange(5, 30))

    remaining_duration_seconds = user.get_module_remaining_duration(module_uuid=module_meta["uuid"])
    total_module_duration_seconds = module_meta["duration"]
    session_end_ts = ts + timedelta(seconds=session_duration)
    while ts < session_end_ts:
        events.append(
            ModuleEvent(
                user,
                ts,
                online,
                module_meta["uuid"],
                ModuleAction.VIEW,
                round(
                    float(total_module_duration_seconds - remaining_duration_seconds) / total_module_duration_seconds
                ),
            )
        )
        ts += timedelta(seconds=300 + random.random() * 300)

    finished = user.progress_module(module_meta["uuid"], session_duration)
    if finished:
        logger.debug("%s finished module: %s", user.get_platform_uuid(), module_meta["uuid"])
        events.append(ModuleEvent(user, ts, online, module_meta["uuid"], ModuleAction.VIEW, 100))

        milestone_events, ts = generate_milestone_and_level_events(
            user, ts, online, module_meta["duration"], block=BlockType.ECOMMERCE
        )
        events.extend(milestone_events)

    return events, ts


def generate_exam_events(user: SyntheticUser, ts: datetime, online: bool) -> Tuple[Sequence[LogEvent], datetime]:
    events: List[LogEvent] = []

    user_profile_conf = user.get_profile_conf()
    min_duration = user_profile_conf.session_length_min_seconds
    max_duration = user_profile_conf.session_length_max_seconds
    session_duration = random.randrange(min_duration, max_duration) if max_duration > min_duration else min_duration

    exam_meta = CatalogCache.get_random_catalog_of_type(CatalogType.EXAM)
    exam_uuid = exam_meta["uuid"]
    exam_difficulty: float = exam_meta["difficulty"]

    question_metas = CatalogCache.get_catalogs_by_properties(CatalogType.QUESTION, {"exam_uuid": exam_uuid})
    average_question_duration_seconds = session_duration / len(question_metas)
    question_duration_min = round(average_question_duration_seconds * 0.9)
    question_duration_max = round(average_question_duration_seconds * 1.2)
    assert len(question_metas) > 0

    exam_profile_conf = user.get_profile_conf().get_event_config(EventType.EXAM)
    pass_probability = exam_profile_conf.properties.get("pass_probability", 0.7)
    skip_probability = exam_profile_conf.properties.get("skip_probability", 0.1)
    abort_probability = exam_profile_conf.properties.get("abort_probability", 0.3)

    final_pass_probability = pass_probability * (1.0 - exam_difficulty)

    aborted = False
    current_score = 0

    initial_ts = ts
    events.append(ExamEvent(user, ts, online, exam_id=exam_uuid, action=ExamAction.START))

    for question_meta in question_metas:
        question_uuid = question_meta["uuid"]
        ts += timedelta(seconds=get_random_int_in_range(question_duration_min, question_duration_max))

        skipped = random.random() < skip_probability

        correct_answer = random.random() < pass_probability
        if correct_answer:
            current_score += 1

        answer_id = (
            question_meta["correct_answer_uuid"]
            if correct_answer
            else random.choice(question_meta["wrong_answer_uuids"])
        )

        action = QuestionAction.ANSWER if not skipped else QuestionAction.SKIP
        events.append(
            QuestionEvent(user, ts, question_id=question_uuid, exam_id=exam_uuid, action=action, answer_id=answer_id)
        )

        aborted = random.random() < abort_probability
        if aborted:
            break

    if not aborted:
        passed = random.random() < final_pass_probability

        ts += timedelta(seconds=get_random_int_in_range(5, 30))
        events.append(
            ExamEvent(
                user,
                ts,
                online,
                exam_id=exam_uuid,
                action=ExamAction.SUBMIT,
                duration=total_difference_seconds(initial_ts, ts),
            )
        )

        ts += timedelta(seconds=get_random_int_in_range(5, 30))
        events.append(
            ExamEvent(
                user,
                ts,
                online,
                exam_id=exam_uuid,
                action=ExamAction.RESULT,
                score=float(current_score) * 100.0 / float(len(question_metas)),
                is_passed=passed,
            )
        )

        rate_events, ts = generate_rate_events(user, ts, exam_uuid, CatalogType.EXAM)
        if len(rate_events) > 0:
            events.extend(rate_events)

    return events, ts


def generate_page_sequence(
    user: SyntheticUser, ts: datetime, online: bool, event_count: int, block: BlockType = BlockType.CORE
) -> Tuple[Sequence[LogEvent], datetime]:
    event_config = user.get_profile_conf().get_event_config(EventType.PAGE)
    duration_seconds_min = event_config.properties.get("duration_seconds_min", 30)
    duration_seconds_max = event_config.properties.get("duration_seconds_max", 60)

    current_ts = ts
    res: List[LogEvent] = []
    page_catalogs = CatalogCache.get_random_unique_catalogs_for_type(CatalogType.PAGE, count=event_count)
    for x in range(0, len(page_catalogs)):
        page_duration_seconds = get_random_float_in_range(duration_seconds_min, duration_seconds_max)

        # We are only able to report the duration of the page view at the end, naturally
        current_ts += timedelta(seconds=page_duration_seconds)

        page_catalog = page_catalogs[x]
        res.append(
            PageEvent(
                user,
                current_ts,
                online,
                uuid=page_catalog["uuid"],
                path=page_catalog["path"],
                title=page_catalog["title"],
                duration=page_duration_seconds,
                block=block,
            )
        )

    return res, current_ts


def generate_search_sequence(
    user: SyntheticUser, ts: datetime, online: bool, page_count: int, block: BlockType = BlockType.CORE
) -> Tuple[Sequence[LogEvent], datetime]:
    event_config = user.get_profile_conf().get_event_config(EventType.SEARCH)
    duration_seconds_min = event_config.properties.get("duration_seconds_min", 30)
    duration_seconds_max = event_config.properties.get("duration_seconds_max", 60)
    results_per_page_max = event_config.properties.get("results_per_page_max", 10)
    assert results_per_page_max >= 1

    search_result_count = page_count * results_per_page_max - get_random_int_in_range(0, results_per_page_max - 1)
    catalog_type_probabilities = user.get_profile_conf().behaviour.purchase.catalog_type_probabilities
    search_result_metas = CatalogCache.get_random_unique_catalogs_from_distribution(
        catalog_type_probabilities, count=search_result_count
    )

    current_ts = ts
    events: List[LogEvent] = []
    query = fake.sentence()
    search_id = f"{str(uuid4())}-{str(current_ts.timestamp())}"

    for page_offset, result_start_index in enumerate(range(0, search_result_count, results_per_page_max)):
        result_end_index = min(result_start_index + results_per_page_max, search_result_count)
        page_result_metas = search_result_metas[result_start_index:result_end_index]

        search_duration = get_random_float_in_range(duration_seconds_min, duration_seconds_max)

        results_list = [ItemObject(result[1]["uuid"], ItemType(result[0].value)) for result in page_result_metas]

        events.append(
            SearchEvent(
                user,
                current_ts,
                online,
                search_id=search_id,
                query=query,
                results_list=results_list,
                page_number=page_offset + 1,
                block=block,
            )
        )

        # Add impressions
        for catalog_type, item_meta in page_result_metas:
            events.append(
                ItemEvent(
                    user,
                    current_ts + timedelta(seconds=1),
                    online=online,
                    shop_item=ItemEvent.build_shop_item_from_meta(item_meta, current_ts + timedelta(seconds=1)),
                    action=ItemAction.IMPRESSION,
                    search_id=search_id,
                )
            )

        current_ts += timedelta(seconds=search_duration)

    return events, current_ts
