# Synthetic Data Generator

## Introduction

The synthetic data generator manages a configured population of synthetic users and send their data to the configured
output. It connects to a configured database and uses it to persist relevant data about the current simulation and the
users.

## Usage

First, you need to install requirements and configure your simulation. You can base it on one of the examples in `data/config`.

Next, you need to make sure the database that your configuration is pointing to is ready and migrated to the latest
version. You go to `src/synthetic/database` and run:

```bash apply_migrations.sh <config_filename>```

This will use `alembic` to create the appropriate tables and upgrade them to the latest version. If you're using the
default configuration, it will create a `.sqlite` database in your `/tmp` directory. You can also configure it to
use any database url that is supported by SQLAlchemy. This is where it persists all the context information about
synthetic users and the synthetic data generation status in order to resume if interrupted. If no `end_ts` is specified,
it will produce logs until the current system time and start producing logs continuously in `online` mode.

You might also find the following scripts useful:

* `drop_tables.py`: drops all the tables used by alembic and the driver to run simulations, if they exist.
* `clear_data.py`: clears all the data from existing tables, in case you want to restart your simulation.

Then you run from `src/synthetic` with:

```python synthetic_data.py <config_filename>```

By default, this will create `.csv` files in `/tmp` containing the logs. 

## Configuration

Documentation on the parameters used in configuration can be found in `src/synthetic/conf/__init__.py`. The
class `GlobalConfig` is the root of all configuration.

## Components

### Driver

The driver is the main entrypoint and responsible for coordinating all activities. It essentially maintains and stores
all the active (and inactive user ids) in a simulation. It is able to persist and resume from persistence at any point
in the simulation.

### Event

A log or catalog event generated by a synthetic user or other actor. These events are cached by the driver, and
occasionally flushed to the configured output.

### User

Represents a unique virtual user which has some customised state. The active users are maintained on the driver and are
individually asked to generate log schedules for themselves as necessary. These logs are then released to the driver as
appropriate by each user to be cached and flushed.

The core of the system is based around synthetic users generating a schedule of events for themselves in some way and
then generating logs from the queue as appropriate. The main concrete strategy is based around session engagement
(`session_engagement_user.py`), which has a hidden engagement factor that changes over time based on the configuration
for the profile. This affects the frequency, number and duration of sessions for a given user.

There is a more specified type of user that layers purchase engagement (`purchase_engagement_user.py`) over session 
engagement. This user generates normal activity, but also e-commerce data based on the configuration. These
implementations can be considered as examples of synthetic user behaviour generation.

### Variable Manager

Represents a variable that can be managed and persisted over time. Just a convenient abstraction that leaves the
persistence out of the hands of the coder and can manage its own update schedule.
