import shutil
from datetime import datetime, timedelta, date
import os
import csv
import json
import logging
from typing import Dict, List, Tuple

from synthetic.conf import global_conf
from synthetic.utils.time_utils import datetime_from_payload_str

logger = logging.getLogger(__name__)


def read_csv_as_list_of_dicts(csv_filename: str) -> List[Dict]:
    logger.info("Loading %s...", csv_filename)

    with open(csv_filename, "r") as csv_file:
        log_events_data = []
        csv_reader = csv.reader(csv_file)

        header = []
        for row_index, row in enumerate(csv_reader):
            if row_index == 0:
                header = row
            else:
                log_events_data.append(dict(zip(header, row)))

    return log_events_data


def load_csv_data_to_validate() -> Tuple[List[Dict], List[Dict]]:
    assert global_conf.log_events_filename is not None
    assert global_conf.catalog_events_filename is not None

    log_events_data = read_csv_as_list_of_dicts(global_conf.log_events_filename)
    catalog_events_filename = read_csv_as_list_of_dicts(global_conf.catalog_events_filename)

    return log_events_data, catalog_events_filename


def load_generated_data() -> Tuple[List[Dict], List[Dict]]:
    if "csv" in global_conf.sink_types:
        log_data, catalog_data = load_csv_data_to_validate()
        logger.info("Validating %s logs and %s catalogs", len(log_data), len(catalog_data))
    else:
        raise ValueError("Cannot validate driver with sink types %s, yet!", global_conf.sink_types)
    return log_data, catalog_data


def dt_from_log(log: Dict) -> datetime:
    dt = datetime_from_payload_str(log["ts"])
    assert dt is not None
    return dt


def mean(row: List[float]) -> float:
    return sum(row) / len(row)


def plot_population(
    all_profile_names: List[str],
    all_plot_dates: List[date],
    types_per_day: Dict[date, Dict[str, List[Dict]]],
    fig_dirname: str,
):
    from matplotlib import pyplot

    counts_per_profile: Dict[str, List] = dict([(profile_name, []) for profile_name in all_profile_names])
    for _date in all_plot_dates:
        if _date not in types_per_day:
            for profile_name in all_profile_names:
                counts_per_profile[profile_name].append(0)
            continue

        date_logs = types_per_day[_date]
        for profile_name in all_profile_names:
            if profile_name not in date_logs:
                counts_per_profile[profile_name].append(0)
            else:
                counts_per_profile[profile_name].append(len(set([log["u_id"] for log in date_logs[profile_name]])))

    fig, ax = pyplot.subplots(1, 1)
    for profile_name in all_profile_names:
        ax.plot(all_plot_dates, counts_per_profile[profile_name], label=profile_name)
    ax.legend()

    fig_filename = os.path.join(fig_dirname, "profile_population.svg")
    fig.savefig(fig_filename)
    pyplot.close()


def plot_individual(logs_per_user: Dict[str, List[Dict]], all_plot_dates: List[date], fig_dirname: str):
    from matplotlib import pyplot

    for user_id, user_logs in logs_per_user.items():
        logger.info("Processing %s...", user_id)
        logs_per_day: Dict[date, List] = {}
        for log in user_logs:
            log_dt = dt_from_log(log)
            log_date = log_dt.date()
            if log_date not in logs_per_day:
                logs_per_day[log_date] = []
            logs_per_day[log_date].append(log)

        y_data = [float(len(logs_per_day[log_date])) if log_date in logs_per_day else 0 for log_date in all_plot_dates]
        y_data_mean = []
        window_size = 7
        for index in range(0, len(y_data)):
            start = max(index - window_size, 0)
            y_data_mean.append(mean(y_data[start : index + 1]))

        fig, ax = pyplot.subplots(1, 1)
        pyplot.scatter([dt_from_log(log) for log in user_logs], [1] * len(user_logs))
        pyplot.xlim([all_plot_dates[0], all_plot_dates[-1]])
        fig_filename = os.path.join(fig_dirname, f"{user_id}_logs.svg")
        fig.suptitle(user_id)
        pyplot.show()

        fig.savefig(fig_filename)
        pyplot.close()

        # fig, ax = pyplot.subplots(1, 1)
        # ax.plot(x_data, y_data_mean)
        # fig_filename = os.path.join(fig_dirname, f"{user_id}_logs_per_day.svg")
        # fig.savefig(fig_filename)
        # pyplot.close()


def validate_generated_data(log_data: List[Dict], catalog_data: List[Dict], plot: bool = True):
    assert len(log_data) > 0
    assert len(catalog_data) > 0

    catalogs_per_type: Dict[str, List[Dict]] = {}
    for catalog_row in catalog_data:
        catalog_type: str = catalog_row["subject_type"]
        if catalog_type not in catalogs_per_type:
            catalogs_per_type[catalog_type] = []

        catalogs_per_type[catalog_type].append(json.loads(catalog_row["data"]))

    logger.info("%s users seen in catalogs", len(catalogs_per_type["user"]))

    all_profile_names_set = set()
    logs_per_user: Dict[str, List[Dict]] = {}
    types_per_day: Dict[date, Dict[str, List[Dict]]] = {}
    for log_row in log_data:
        user_id = log_row["u_id"]
        if user_id not in logs_per_user:
            logs_per_user[user_id] = []
        logs_per_user[user_id].append(log_row)

        profile_name = user_id.split('-')[0]
        all_profile_names_set.add(profile_name)
        dt = dt_from_log(log_row)
        dt_date = dt.date()
        if dt_date not in types_per_day:
            types_per_day[dt_date] = {}
        if profile_name not in types_per_day[dt_date]:
            types_per_day[dt_date][profile_name] = []
        types_per_day[dt_date][profile_name].append(log_row)
    all_profile_names: List[str] = sorted(list(all_profile_names_set))

    logger.info("%s users seen in logs", len(logs_per_user))

    all_dates = sorted(list(set([dt_from_log(log).date() for log in log_data])))
    all_plot_dates = [all_dates[0]]
    current_date = all_dates[0]
    while current_date <= all_dates[-1]:
        current_date += timedelta(days=1)
        all_plot_dates.append(current_date)

    if plot:
        fig_dirname = "figs"
        if os.path.exists(fig_dirname):
            shutil.rmtree(fig_dirname)
        os.makedirs(fig_dirname)

        individual = True
        population = True

        if population:
            plot_population(all_profile_names, all_plot_dates, types_per_day, fig_dirname)

        if individual:
            plot_individual(logs_per_user, all_plot_dates, fig_dirname)
