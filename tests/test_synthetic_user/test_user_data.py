from synthetic.utils.user_utils import generate_random_user_data


def test_generate_random_user_data(fixed_seed):
    data = generate_random_user_data()
    assert sorted(data.keys()) == [
        'city',
        'country',
        'education_level',
        'email',
        'experience',
        'language',
        'name',
        "organization",
        'platform_uuid',
        'profession',
        'region_state',
        'timezone',
        'workplace',
        'zipcode',
    ]
