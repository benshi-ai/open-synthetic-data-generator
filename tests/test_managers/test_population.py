from datetime import datetime, timedelta
from synthetic.conf import PopulationConfig, global_conf
from synthetic.managers.population import PopulationManager


def test_basic_limits():
    start_ts = datetime(2000, 1, 1, 0, 0, 0)
    global_conf.start_ts = start_ts

    data = {}
    population_manager = PopulationManager(
        stored_data=data,
        config=PopulationConfig(initial_count=100, target_min_count=1, target_max_count=200, volatility=0.05),
        initial_ts=start_ts,
    )
    population_manager.initialize()

    population_history = []
    for day_offset in range(0, 1200):
        current_ts = start_ts + timedelta(days=1 + day_offset)

        population_manager.update(current_ts)
        latest_population = population_manager.get_population()

        # Make sure that many calls on the same ts doesn't change the population
        population_manager.update(current_ts + timedelta(seconds=60))

        population_history.append(latest_population)

        current_ts += timedelta(days=1)

    # pyplot.plot(population_history)
    # pyplot.show()

    assert 1 <= min(population_history) < 100
    assert 100 <= max(population_history) < 200
