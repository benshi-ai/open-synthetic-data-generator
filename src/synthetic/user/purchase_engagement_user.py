import logging
import random

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import global_conf, PurchaseBehaviourConfig
from synthetic.constants import CatalogType, BlockType
from synthetic.database.schemas import SyntheticUserSchema
from synthetic.event.constants import PaymentType
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.commerce.cancel_checkout import CancelType, CancelCheckoutEvent
from synthetic.event.log.commerce.cart import CartEvent, CartAction
from synthetic.event.log.commerce.checkout import CheckoutEvent
from synthetic.event.log.commerce.constants import ShopItem, ItemType
from synthetic.event.log.loyalty.promo import PromoAction, PromoEvent
from synthetic.event.log.payment.deferred_payment import DeferredPaymentEvent, DeferredPaymentAction
from synthetic.event.log.commerce.item import ItemEvent, ItemAction
from synthetic.event.log.generator import generate_milestone_and_level_events
from synthetic.event.log.log_base import LogEvent
from synthetic.user.constants import SyntheticUserType
from synthetic.managers.engagement import EngagementManager
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.random import get_random_float_in_range, get_random_int_in_range
from synthetic.utils.user_utils import create_user_platform_uuid

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class PurchaseEngagementUser(SessionEngagementUser):
    """A specialised session engagement user that also has a hidden purchase engagement, which determines the likelihood
    of a user viewing and/or purchasing something during a session.

    """

    @classmethod
    def create_random_user(
        cls,
        driver_meta_id: int,
        registration_ts: datetime,
        profile_name: str,
        platform_uuid: Optional[str] = None,
    ):
        assert isinstance(driver_meta_id, int)

        if platform_uuid is None:
            platform_uuid = create_user_platform_uuid(profile_name)

        user = PurchaseEngagementUser(
            driver_meta_id,
            platform_uuid,
            cls.create_initial_profile_data(profile_name, registration_ts=registration_ts),
        )
        return user

    @staticmethod
    def from_db_data(raw_data: SyntheticUserSchema) -> SyntheticUser:
        return PurchaseEngagementUser(
            driver_meta_id=raw_data.driver_meta_id,
            platform_uuid=raw_data.platform_uuid,
            profile_data=raw_data.profile_data,
            last_seen_ts=raw_data.last_seen_ts,
        )

    def __init__(
        self,
        driver_meta_id: int,
        platform_uuid: str,
        profile_data: Dict,
        last_seen_ts: Optional[datetime] = None,
    ):
        super().__init__(driver_meta_id, platform_uuid, profile_data, last_seen_ts)

        self._type = SyntheticUserType.PURCHASE_ENGAGEMENT

        purchase_engagement_config = self.get_profile_conf().get_engagement_config("purchase_engagement")
        purchase_engagement_manager = EngagementManager(
            profile_data, purchase_engagement_config, self.registration_ts, "purchase_engagement"
        )
        purchase_engagement_manager.initialize()
        self.add_manager(purchase_engagement_manager)

    @property
    def _purchase_engagement_level(self) -> Optional[float]:
        if self.get_purchase_engagement_manager() is None:
            return None

        return self.get_purchase_engagement_manager().get_engagement()

    def get_purchase_engagement_manager(self) -> EngagementManager:
        return self.get_manager("purchase_engagement")

    def get_item_interests(self) -> Dict[str, Dict]:
        if "item_interests" not in self._profile_data:
            self.set_profile_data_value("item_interests", {})

        return self._profile_data["item_interests"]

    @property
    def account_balance(self) -> float:
        if "current_account_balance" not in self._profile_data:
            self.set_profile_data_value("current_account_balance", 0.0)

        return self._profile_data["current_account_balance"]

    def set_account_balance(self, account_balance: float, current_ts: datetime):
        self.set_profile_data_value("current_account_balance", account_balance, change_ts=current_ts)

    def get_preferred_payment_type(self) -> PaymentType:
        user_data = self.get_profile_data()
        if "preferred_payment_type" not in user_data:
            preferred_payment_type = random.choice(list(PaymentType))
            user_data["preferred_payment_type"] = preferred_payment_type.name
        else:
            preferred_payment_type = PaymentType[user_data["preferred_payment_type"]]

        return preferred_payment_type

    def _manage_account_balance(
        self, current_ts: datetime, config: PurchaseBehaviourConfig
    ) -> Tuple[datetime, List[DeferredPaymentEvent]]:

        events: List[DeferredPaymentEvent] = []

        if "payment_failure_probability" not in self._profile_data:
            payment_failure_probability = get_random_float_in_range(
                config.payment_failure_probability_min, config.payment_failure_probability_max
            )
            self.set_profile_data_value(
                "payment_failure_probability", payment_failure_probability, change_ts=current_ts
            )
        else:
            payment_failure_probability = self._profile_data["payment_failure_probability"]

        payment_amount = 0.0
        payment_successful = True
        if "current_account_balance" not in self._profile_data:
            # Initial account setup
            payment_amount = get_random_float_in_range(
                config.initial_account_balance_min, config.initial_account_balance_max
            )
            self.set_profile_data_value("current_account_balance", 0.0, change_ts=current_ts)
        elif self.account_balance < config.initial_account_balance_min and random.random() < config.top_up_probability:
            # Top up account
            payment_amount = (
                1.0
                + round(
                    get_random_float_in_range(
                        config.initial_account_balance_min / 100, config.initial_account_balance_max / 100
                    )
                )
            ) * 100.0
            payment_successful = random.random() < payment_failure_probability

        if payment_amount > 0:
            if payment_successful:
                self.set_account_balance(self.account_balance + payment_amount, current_ts=current_ts)

            payment_id = str(uuid4())
            order_id = str(uuid4())
            self.get_profile_data()
            preferred_payment_type = self.get_preferred_payment_type()

            events.append(
                DeferredPaymentEvent(
                    self,
                    current_ts,
                    payment_id,
                    action=DeferredPaymentAction.PAYMENT_PROCESSED,
                    payment_type=preferred_payment_type,
                    account_balance=self.account_balance,
                    payment_amount=payment_amount,
                    is_successful=payment_successful,
                    order_id=order_id,
                )
            )
            current_ts += timedelta(seconds=get_random_int_in_range(1, 200))

        return current_ts, events

    def get_purchase_behaviour_config(self) -> PurchaseBehaviourConfig:
        return self.get_profile_conf().behaviour.purchase

    def _update_item_interests(self, current_ts: datetime):
        item_interests = self.get_item_interests()
        purchase_behaviour_config = self.get_purchase_behaviour_config()
        catalog_probabilities = purchase_behaviour_config.catalog_type_probabilities

        if len(item_interests) == 0:
            # Assume we have populated catalog
            shop_item_catalog_count = sum(
                [global_conf.get_catalog_config(catalog_type).target_count for catalog_type in catalog_probabilities]
            )

            interested_item_count = round(
                shop_item_catalog_count
                * get_random_float_in_range(
                    purchase_behaviour_config.interest_catalog_range_min,
                    purchase_behaviour_config.interest_catalog_range_max,
                )
            )

            item_interests = {}

            interested_item_catalogs = CatalogCache.get_random_unique_catalogs_from_distribution(
                catalog_probabilities, interested_item_count
            )
            for interested_catalog_type, interested_item_catalog in interested_item_catalogs:
                item_interests[interested_item_catalog["uuid"]] = {
                    "catalog_type": interested_catalog_type.value,
                    "interest_ratio": get_random_float_in_range(
                        purchase_behaviour_config.interest_per_item_min,
                        purchase_behaviour_config.interest_per_item_max,
                    ),
                }

        for uuid, item_interest in item_interests.items():
            if "catalog_type" not in item_interest:
                item_interest["catalog_type"] = CatalogType.DRUG.value

            remaining_views = item_interest.get("remaining_view_count", 0)
            if remaining_views <= 0:
                remaining_views = get_random_int_in_range(
                    purchase_behaviour_config.views_required_per_purchase_min,
                    purchase_behaviour_config.views_required_per_purchase_max,
                )

                item_interest["remaining_view_count"] = remaining_views

        self.set_profile_data_value("item_interests", item_interests, change_ts=current_ts)

    def _generate_reminder_events(
        self, current_ts: datetime, shop_item: ShopItem, online: bool
    ) -> Tuple[datetime, List[LogEvent]]:

        current_reminders: List[str] = self._profile_data.get("current_reminders", [])

        reminder_events: List[LogEvent] = []
        if shop_item.id not in current_reminders:
            reminder_events.append(ItemEvent(self, current_ts, online, shop_item, action=ItemAction.ADD_REMINDER))
            current_reminders.append(shop_item.id)
        else:
            auto_reminder_type_probability = self.get_profile_conf().behaviour.purchase.auto_reminder_type_probability
            action: ItemAction = (
                ItemAction.REMOVE_REMINDER
                if random.random() > auto_reminder_type_probability
                else ItemAction.REMOVE_REMINDER_AUTO
            )
            reminder_events.append(ItemEvent(self, current_ts, online, shop_item, action=action))
            current_reminders.remove(shop_item.id)

        self.set_profile_data_value("current_reminders", current_reminders, change_ts=current_ts)
        current_ts += timedelta(seconds=random.randrange(5, 30))

        return current_ts, reminder_events

    def _generate_favorite_events(
        self, current_ts: datetime, shop_item: ShopItem, online: bool
    ) -> Tuple[datetime, List[LogEvent]]:

        current_favorites: List[str] = self._profile_data.get("current_favorites", [])

        favorite_events: List[LogEvent] = []
        if shop_item.id not in current_favorites:
            favorite_events.append(ItemEvent(self, current_ts, online, shop_item, action=ItemAction.ADD_FAVORITE))
            current_favorites.append(shop_item.id)
        else:
            favorite_events.append(ItemEvent(self, current_ts, online, shop_item, action=ItemAction.REMOVE_FAVORITE))
            current_favorites.remove(shop_item.id)

        self.set_profile_data_value("current_favorites", current_favorites, change_ts=current_ts)
        current_ts += timedelta(seconds=random.randrange(5, 30))

        return current_ts, favorite_events

    def _update_checkout_behaviour_probabilities(self, session_start_ts: datetime):
        purchase_behaviour_config = self.get_purchase_behaviour_config()
        if "checkout_failure_probability" not in self._profile_data:
            checkout_failure_probability = get_random_float_in_range(
                purchase_behaviour_config.checkout_failure_probability_min,
                purchase_behaviour_config.checkout_failure_probability_max,
            )
            self.set_profile_data_value(
                "checkout_failure_probability", checkout_failure_probability, change_ts=session_start_ts
            )
        if "checkout_urgent_probability" not in self._profile_data:
            checkout_urgent_probability = get_random_float_in_range(
                purchase_behaviour_config.checkout_urgent_probability_min,
                purchase_behaviour_config.checkout_urgent_probability_max,
            )
            self.set_profile_data_value(
                "checkout_urgent_probability", checkout_urgent_probability, change_ts=session_start_ts
            )
        if "checkout_cancellation_probability" not in self._profile_data:
            checkout_cancellation_probability = get_random_float_in_range(
                purchase_behaviour_config.checkout_cancellation_probability_min,
                purchase_behaviour_config.checkout_cancellation_probability_max,
            )
            self.set_profile_data_value(
                "checkout_cancellation_probability", checkout_cancellation_probability, change_ts=session_start_ts
            )
        if "checkout_promo_probability" not in self._profile_data:
            checkout_promo_probability = get_random_float_in_range(
                purchase_behaviour_config.checkout_promo_probability_min,
                purchase_behaviour_config.checkout_promo_probability_max,
            )
            self.set_profile_data_value(
                "checkout_promo_probability", checkout_promo_probability, change_ts=session_start_ts
            )

    def _generate_item_impressions(self, current_ts: datetime, online: bool) -> List[LogEvent]:
        purchase_behaviour_config = self.get_purchase_behaviour_config()

        log_events: List[LogEvent] = []
        # Some initial impressions
        impression_count = round(
            get_random_int_in_range(
                purchase_behaviour_config.views_per_session_min,
                purchase_behaviour_config.views_per_session_max,
            )
            * purchase_behaviour_config.impression_ratio
        )

        catalog_probabilities = purchase_behaviour_config.catalog_type_probabilities
        assert len(catalog_probabilities) > 0
        item_metas = CatalogCache.get_random_unique_catalogs_from_distribution(catalog_probabilities, impression_count)

        for catalog_type, item_meta in item_metas:
            log_events.append(
                ItemEvent(
                    self,
                    current_ts,
                    online=online,
                    shop_item=ItemEvent.build_shop_item_from_meta(item_meta, current_ts),
                    action=ItemAction.IMPRESSION,
                )
            )

        return log_events

    def _generate_promo_events(self, promo_ids: List[str], current_ts: datetime, online: bool) -> List[PromoEvent]:
        promo_events: List[PromoEvent] = []
        for promo_id in promo_ids:
            promo_catalog = CatalogCache.get_catalog_by_uuid(CatalogType.PROMO, promo_id)
            promo_events.append(
                PromoEvent.build_from_catalog(self, current_ts, online, promo_catalog, action=PromoAction.APPLY)
            )

        return promo_events

    def _generate_order_events(
        self,
        order_items: List[ShopItem],
        current_ts: datetime,
        online: bool,
        cart_id: str,
        total_price: float,
        checkout_successful: bool,
    ) -> List[LogEvent]:
        purchase_behaviour_config = self.get_purchase_behaviour_config()
        checkout_urgent_probability = self._profile_data["checkout_urgent_probability"]
        checkout_cancellation_probability = self._profile_data["checkout_cancellation_probability"]

        log_events: List[LogEvent] = []
        order_id = str(uuid4())
        is_urgent = random.random() < checkout_urgent_probability
        will_be_cancelled = random.random() < checkout_cancellation_probability
        cancellation_type: CancelType = random.choice(list(CancelType))
        update_event_count = get_random_int_in_range(
            purchase_behaviour_config.update_events_per_checkout_min,
            purchase_behaviour_config.update_events_per_checkout_max,
        )

        promo_ids = sorted(
            list(set([order_item.promo_id for order_item in order_items if order_item.promo_id is not None]))
        )
        if len(promo_ids) > 0:
            log_events.extend(self._generate_promo_events(promo_ids, current_ts, online))

        if will_be_cancelled and cancellation_type == CancelType.CART:
            log_events.append(
                CancelCheckoutEvent(
                    self,
                    current_ts,
                    online,
                    order_id,
                    cancellation_type,
                    items=[item.get_item_object() for item in order_items],
                    reason="Unknown",
                )
            )
        else:
            log_events.append(
                CheckoutEvent(
                    self,
                    current_ts,
                    online,
                    order_id,
                    total_price,
                    order_items,
                    is_urgent=is_urgent,
                    cart_id=cart_id,
                    is_successful=checkout_successful,
                    will_be_cancelled=will_be_cancelled,
                    update_event_count=update_event_count,
                )
            )

        if not will_be_cancelled:
            milestone_events, current_ts = generate_milestone_and_level_events(
                self, current_ts, online, total_price, block=BlockType.ECOMMERCE
            )
            log_events.extend(milestone_events)

        return log_events

    def _create_events_for_session(
        self,
        session_start_ts: datetime,
        min_session_duration_seconds: int,
        online: Optional[bool] = None,
    ) -> Tuple[EventCollection, datetime]:
        if online is None:
            online = random.random() < self.get_profile_conf().online_probability

        purchase_behaviour_config = self.get_purchase_behaviour_config()
        self._update_checkout_behaviour_probabilities(session_start_ts)

        checkout_failure_probability = self._profile_data["checkout_failure_probability"]
        checkout_promo_probability = self._profile_data["checkout_promo_probability"]

        checkout_successful = random.random() >= checkout_failure_probability

        log_events: List[LogEvent] = []
        current_ts = session_start_ts
        if random.random() < self.get_profile_conf().behaviour.normal_event_probability:
            # This guy first does some normal stuff
            events, current_ts = super()._create_events_for_session(current_ts, min_session_duration_seconds, online)
            log_events = events.log_events

        # And now his specialized behaviour
        current_ts = session_start_ts
        self._update_item_interests(current_ts)

        current_ts, deferred_payment_events = self._manage_account_balance(current_ts, purchase_behaviour_config)
        log_events.extend(deferred_payment_events)

        log_events.extend(self._generate_item_impressions(current_ts, online))

        # Then we are in view mode
        item_interests = self.get_item_interests()
        relevant_item_uuids_for_session: List[Tuple[CatalogType, str]] = []
        for item_uuid, item_interest in item_interests.items():
            if random.random() < item_interest["interest_ratio"]:
                relevant_item_uuids_for_session.append((CatalogType(item_interest["catalog_type"]), item_uuid))

        item_views: List[Tuple[CatalogType, str]] = []
        for catalog_type, item_uuid in relevant_item_uuids_for_session:
            view_count = get_random_int_in_range(
                purchase_behaviour_config.views_per_session_min,
                purchase_behaviour_config.views_per_session_max,
            )
            item_views.extend([(catalog_type, item_uuid)] * view_count)

            item_interests[item_uuid]["remaining_view_count"] -= view_count

        if len(item_views) > 0:
            # We viewed some stuff
            random.shuffle(item_views)
            for catalog_type, item_uuid in item_views:
                item_meta = CatalogCache.get_catalog_by_uuid(catalog_type, item_uuid)
                shop_item = ItemEvent.build_shop_item_from_meta(item_meta, current_ts)

                # We have an impression of an interesting item
                log_events.append(
                    ItemEvent(self, current_ts + timedelta(seconds=1), online, shop_item, ItemAction.IMPRESSION)
                )
                # We have a view of an interesting item
                log_events.append(
                    ItemEvent(self, current_ts + timedelta(seconds=2), online, shop_item, ItemAction.VIEW)
                )

                current_ts += timedelta(seconds=random.randrange(5, 30))

                if random.random() < purchase_behaviour_config.detail_probability:
                    # And in extreme cases, we view detail!
                    log_events.append(ItemEvent(self, current_ts, online, shop_item, ItemAction.DETAIL))
                    current_ts += timedelta(seconds=random.randrange(5, 30))

                if random.random() < purchase_behaviour_config.favorite_probability:
                    current_ts, favorite_events = self._generate_favorite_events(current_ts, shop_item, online)
                    log_events.extend(favorite_events)

                if random.random() < purchase_behaviour_config.reminder_probability:
                    current_ts, reminder_events = self._generate_reminder_events(current_ts, shop_item, online)
                    log_events.extend(reminder_events)

        # We view some promos
        promo_view_count = get_random_int_in_range(
            purchase_behaviour_config.views_per_session_min,
            purchase_behaviour_config.views_per_session_max,
        )
        promo_catalogs = CatalogCache.get_random_unique_catalogs_for_type(CatalogType.PROMO, promo_view_count)
        for promo_catalog in promo_catalogs:
            log_events.append(
                PromoEvent.build_from_catalog(self, current_ts, online, promo_catalog, action=PromoAction.VIEW)
            )
            current_ts += timedelta(seconds=random.randint(1, 5))

        # Now we are in purchase mode!
        total_price = 0.0
        cart_id = str(uuid4())
        order_items: List[ShopItem] = []

        for item_uuid, item_interest in item_interests.items():
            if item_interests[item_uuid]["remaining_view_count"] <= 0:
                # We've seen enough! BUY BUY BUY
                purchase_count = get_random_int_in_range(
                    purchase_behaviour_config.purchase_count_per_item_min,
                    purchase_behaviour_config.purchase_count_per_item_max,
                )

                item_meta = CatalogCache.get_catalog_by_uuid(CatalogType(item_interest["catalog_type"]), item_uuid)
                try_promo = random.random() < checkout_promo_probability
                item_uuid = item_meta["uuid"]
                item_type = ItemType(item_meta["type"])

                promo_tuple: Optional[Tuple[str, float]] = None
                if (
                    try_promo
                    and item_type in CatalogCache.current_promotions
                    and item_uuid in CatalogCache.current_promotions[item_type]
                ):
                    # We have promotions and want to use them!
                    promo_tuple = random.choice(CatalogCache.current_promotions[item_type][item_uuid])

                order_item = ItemEvent.build_shop_item_from_meta(
                    item_meta, current_ts, quantity=purchase_count, promo_tuple=promo_tuple
                )

                order_items.append(order_item)
                total_price += purchase_count * order_item.item_price

                log_events.append(CartEvent(self, current_ts, cart_id, CartAction.ADD_ITEM, order_item, total_price))

                if checkout_successful:
                    self.set_account_balance(self.account_balance - total_price, current_ts=current_ts)

                current_ts += timedelta(seconds=random.randrange(5, 30))

        order_items = sorted(order_items, key=lambda curr_item: curr_item.id)

        if len(order_items) > 0:
            log_events.extend(
                self._generate_order_events(order_items, current_ts, online, cart_id, total_price, checkout_successful)
            )

        return EventCollection(log_events=log_events), current_ts
