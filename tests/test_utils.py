import pytest

from synthetic.conf import (
    ProfileConfig,
)
from synthetic.utils.random import build_need_based_profile_probabilities

desired_population_count = 100


@pytest.mark.parametrize(
    "input_existing_counts,input_profiles,expected_probabilities",
    [
        ({}, {"plentiful": ProfileConfig(occurrence_probability=1.0)}, {"plentiful": 1.0}),
        (
            {"balanced_a": desired_population_count * 0.5},
            {
                "balanced_a": ProfileConfig(occurrence_probability=0.5),
                "balanced_b": ProfileConfig(occurrence_probability=0.5),
            },
            {"balanced_a": 0.0, "balanced_b": 1.0},
        ),
        (
            {"balanced_a": desired_population_count * 0.5, "balanced_b": desired_population_count * 0.5},
            {
                "balanced_a": ProfileConfig(occurrence_probability=0.5),
                "balanced_b": ProfileConfig(occurrence_probability=0.5),
            },
            {"balanced_a": 0.5, "balanced_b": 0.5},
        ),
        (
            {"balanced_a": desired_population_count * 0.5, "balanced_b": desired_population_count * 0.5},
            {
                "balanced_a": ProfileConfig(occurrence_probability=1.0),
                "balanced_b": ProfileConfig(occurrence_probability=1.0),
            },
            {"balanced_a": 0.5, "balanced_b": 0.5},
        ),
        (
            {"balanced_a": desired_population_count, "balanced_b": desired_population_count * 0},
            {
                "balanced_a": ProfileConfig(occurrence_probability=1.0),
                "balanced_b": ProfileConfig(occurrence_probability=1.0),
            },
            {"balanced_a": 0.0, "balanced_b": 1.0},
        ),
        (
            {"balanced_a": desired_population_count, "balanced_b": 0, "balanced_c": 0},
            {
                "balanced_a": ProfileConfig(occurrence_probability=1.0),
                "balanced_b": ProfileConfig(occurrence_probability=1.0),
                "balanced_c": ProfileConfig(occurrence_probability=1.0),
            },
            {"balanced_a": 0.0, "balanced_b": 0.5, "balanced_c": 0.5},
        ),
    ],
)
def test_build_need_based_profile_probabilities(input_existing_counts, input_profiles, expected_probabilities):
    found_probabilities = build_need_based_profile_probabilities(
        desired_population_count, input_existing_counts, input_profiles
    )
    assert found_probabilities == expected_probabilities


def test_realistic_need_based_profile_probabilities():
    found_probabilities = build_need_based_profile_probabilities(
        desired_population_count,
        {"average": 26, "long": 19, "loyal": 18, "one_time": 7, "short": 31},
        {
            "average": ProfileConfig(occurrence_probability=1.0),
            "long": ProfileConfig(occurrence_probability=1.0),
            "loyal": ProfileConfig(occurrence_probability=1.0),
            "one_time": ProfileConfig(occurrence_probability=1.0),
            "short": ProfileConfig(occurrence_probability=1.0),
        },
    )
    assert found_probabilities == {
        'average': 0.0,
        'long': 0.06250000000000001,
        'loyal': 0.12500000000000003,
        'one_time': 0.8125,
        'short': 0.0,
    }
