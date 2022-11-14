from typing import Any, List, Dict


def set_variable_in_path(data: Dict, path: str, value: Any):
    current_data = data
    parts = path.split("/")
    if len(parts) == 0:
        raise ValueError("Could not parse: %s" % (path,))
    elif len(parts) > 1:
        for part in parts[0:-1]:
            if part not in current_data:
                current_data[part] = {}
            current_data = current_data[part]

    current_data[parts[-1]] = value


class BaseUpdate:
    def apply_to_user(self, user: "SyntheticUser"):  # type: ignore
        raise NotImplementedError()


class SetVariableUpdate(BaseUpdate):
    def __init__(self, path: str, value: Any):
        self._path = path
        self._value = value

    def apply_to_user(self, user: "SyntheticUser"):  # type: ignore
        profile_data = user.get_profile_data()
        set_variable_in_path(profile_data, self._path, self._value)


class ProfileDataUpdate:
    @staticmethod
    def create_variable_set_update(path: str, value: Any) -> "ProfileDataUpdate":
        update = ProfileDataUpdate()
        update.add_set_variable(path, value)
        return update

    def __init__(self):
        self._updates: List[BaseUpdate] = []

    def add_set_variable(self, path, value):
        self._updates.append(SetVariableUpdate(path, value))

    def apply_to_user(self, user: "SyntheticUser"):  # type: ignore
        for update in self._updates:
            update.apply_to_user(user)
