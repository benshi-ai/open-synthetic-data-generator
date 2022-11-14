from datetime import datetime, timedelta
from synthetic.conf import EngagementConfig, global_conf
from synthetic.managers.engagement import EngagementManager
from synthetic.managers.managed_object import ManagedObject


def test_basic_limits():
    start_ts = datetime(2000, 1, 1)
    global_conf.start_ts = start_ts

    data = {}

    user_count_per_class = 10

    manager = ManagedObject()
    for user_index in range(0, user_count_per_class):
        manager.add_manager(
            EngagementManager(
                stored_data=data,
                config=EngagementConfig(
                    initial_min=1.0,
                    initial_max=1.0,
                    change_probability=0.9,
                    boost_probability=0.0,
                    decay_probability=0.9,
                    change_min=0.5,
                    change_max=1.0,
                ),
                variable_name=f"one-time-{user_index}",
                initial_ts=start_ts,
            )
        )
        manager.add_manager(
            EngagementManager(
                stored_data=data,
                config=EngagementConfig(
                    initial_min=1.0,
                    initial_max=1.0,
                    change_probability=0.6,
                    boost_probability=0.2,
                    decay_probability=0.8,
                    change_min=0.1,
                    change_max=0.2,
                ),
                variable_name=f"short-{user_index}",
                initial_ts=start_ts,
            )
        )
        manager.add_manager(
            EngagementManager(
                stored_data=data,
                config=EngagementConfig(
                    initial_min=1.0,
                    initial_max=1.0,
                    change_probability=0.4,
                    boost_probability=0.35,
                    decay_probability=0.65,
                    change_min=0.05,
                    change_max=0.15,
                ),
                variable_name=f"average-{user_index}",
                initial_ts=start_ts,
            )
        )
        manager.add_manager(
            EngagementManager(
                stored_data=data,
                config=EngagementConfig(
                    initial_min=1.0,
                    initial_max=1.0,
                    change_probability=0.25,
                    boost_probability=0.35,
                    decay_probability=0.65,
                    change_min=0.02,
                    change_max=0.1,
                ),
                variable_name=f"long-{user_index}",
                initial_ts=start_ts,
            )
        )
        manager.add_manager(
            EngagementManager(
                stored_data=data,
                config=EngagementConfig(
                    initial_min=1.0,
                    initial_max=1.0,
                    change_probability=0.2,
                    boost_probability=0.45,
                    decay_probability=0.55,
                    change_min=0.01,
                    change_max=0.05,
                ),
                variable_name=f"loyal-{user_index}",
                initial_ts=start_ts,
            )
        )
    manager.initialize_all()

    variable_names = ["one-time", "short", "average", "long", "loyal"]
    engagement_histories = {}
    for day_offset in range(0, 320):
        for variable_name in variable_names:
            for user_index in range(0, user_count_per_class):
                user_id = f"{variable_name}-{user_index}"
                if user_id not in engagement_histories:
                    engagement_histories[user_id] = []
                engagement_histories[user_id].append(manager.get_manager(user_id).get_engagement())

        current_ts = start_ts + timedelta(days=1 + day_offset)

        manager.update_managers(current_ts)

        current_ts += timedelta(days=1)

    # import matplotlib.pyplot as pyplot
    colors = ['r', 'g', 'b', 'y', 'm']
    for variable_name, color in zip(variable_names, colors):
        for user_index in range(0, user_count_per_class):
            user_id = f"{variable_name}-{user_index}"

            engagement_history = engagement_histories[user_id]
            assert 0.0 <= engagement_history[-1] <= 1.0
            # pyplot.plot(engagement_history, label=variable_name, color=color)

    # pyplot.legend()
    # pyplot.show()
