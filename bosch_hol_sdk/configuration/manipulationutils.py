from collections import OrderedDict
import dataclasses
import json
import logging
import operator
import warnings

from .manipulationconfig import (
    ReplayManipulationType, LidarManipulationUnit, SomeIpManipulationUnit,
    CanManipulationUnit, CameraManipulationUnit,
    ReplayManipulationStep,  # noqa: F401
)
from .exceptions import (
    ReplayManipulationWithoutStepsError, UnsupportedManipulationTypeError,
)


# Logger for the whole module
_logger = logging.getLogger('DRAPI.datamanipulation')


# #############################################################################
# Data Manipulation object checks
#
# NOTE: These functions raise exceptions if any problems are detected!
#
#       For the sake of consistency, all these functions return a list of the
#       manipulation units even if they never change anything (at least one
#       might).
# #############################################################################
def _warn(logger, message):
    logger.warning(message)
    warnings.warn(message)


def check_types(manipulation_units):
    manipulation_type_support = {
        CanManipulationUnit: [
            ReplayManipulationType.OVERWRITE,
            ReplayManipulationType.SUSPEND,
            ReplayManipulationType.CRC,
            ReplayManipulationType.ROLLING_COUNTER,
        ],
        LidarManipulationUnit: [
            ReplayManipulationType.SUSPEND,
        ],
        SomeIpManipulationUnit: [
            ReplayManipulationType.OVERWRITE,
            ReplayManipulationType.SUSPEND,
            ReplayManipulationType.SIMULATE,
        ],
        CameraManipulationUnit: [
            ReplayManipulationType.SUSPEND,
        ],
    }
    for unit in manipulation_units:
        if not unit.steps:
            raise ReplayManipulationWithoutStepsError(unit)
        try:
            support_list = manipulation_type_support[unit.__class__]
        except KeyError as exc:
            # This is an internal error that should never happen.
            raise NotImplementedError(unit.__class__) from exc
        if unit.type not in support_list:
            raise UnsupportedManipulationTypeError(unit, support_list)

    return manipulation_units


def is_monostep_only(manipulation_type: ReplayManipulationType):
    mono_step_types = [
        ReplayManipulationType.SUSPEND,
        ReplayManipulationType.ROLLING_COUNTER,
    ]
    return manipulation_type in mono_step_types


def check_steps(manipulation_units):
    for unit in manipulation_units:
        if not unit.steps:
            raise ValueError('A manipulation unit with empty steps detected!')

        count = len(unit.steps)
        if is_monostep_only(unit.type):
            if count != 1:
                raise ValueError(
                    f'Manipulation of type {unit.type} supports only one '
                    f'step ({count} provided)!'
                )
        else:
            if count > 10:
                raise ValueError(
                    f'Cannot use more than 10 steps ({count} provided)!'
                )

            sorted_steps = sorted(unit.steps, key=operator.attrgetter('start'))
            if unit.steps != sorted_steps:
                _warn(
                    _logger.getChild('check_steps'),
                    f'Unsorted steps detected for {unit}: {unit.steps}',
                )
                # Resorting is disabled for now.
                # unit.steps[:] = sorted_steps
    return manipulation_units


def check_redundancies(manipulation_units):
    # Use a dictionary for sorting since keys cannot be repeated.
    redundancy_dict = OrderedDict()
    for unit in manipulation_units:
        if unit in redundancy_dict:
            # pop the old unit to replace it.
            count = redundancy_dict.pop(unit)
        else:
            count = 0
        redundancy_dict[unit] = count + 1

    for unit, count in redundancy_dict.items():
        if count > 1:
            _warn(
                _logger.getChild('check_redundancies'),
                (
                    f'Detected {count} redundancies for unit {unit}. '
                    'Only the last configuration is taken.'
                ),
            )

    return list(redundancy_dict)


def check_someip_consistencies(manipulation_units):
    filtered_units = {}
    for unit in manipulation_units:
        if (
            isinstance(unit, SomeIpManipulationUnit) and
            unit.type != ReplayManipulationType.SUSPEND
        ):
            # Build a tuple of the elements defining an event.
            key = (
                unit.service_name,
                unit.service_id,
                unit.event_group,
                unit.event,
            )
            try:
                filtered_units[key].append(unit)
            except KeyError:
                filtered_units[key] = [unit]

    for units in filtered_units.values():
        # create a set of tuples of the steps' start and duraction.
        steps = {
            tuple((step.start, step.duration) for step in unit.steps)
            for unit in units
        }
        if len(steps) > 1:
            raise ValueError(
                'Inconsistent SOME/IP event manipulation detected! '
                'Affected configuration units:\n\t'
                + '\n\t'.join(str(unit) for unit in units)
            )

    return manipulation_units


def check_all(manipulation_units):
    manipulation_units = check_types(manipulation_units)
    manipulation_units = check_steps(manipulation_units)
    manipulation_units = check_redundancies(manipulation_units)
    manipulation_units = check_someip_consistencies(manipulation_units)
    return manipulation_units


# #############################################################################
# Deserialization Code
# #############################################################################
def _translate_json_objects(json_obj: dict):
    """ Convert the json object to a dataclass if it has a _cls_ member. """
    try:
        cls_name = json_obj.pop('_cls_')
    except KeyError:
        return json_obj

    try:
        cls = globals()[cls_name]
    except KeyError:
        raise NotImplementedError(cls_name)

    type_map = {f.name: f.type for f in dataclasses.fields(cls)}
    for k, v in json_obj.items():
        json_obj[k] = type_map[k](_load_from_json(v))
    obj = cls(**json_obj)
    return obj


def _load_from_json(json_string: str):
    """ convert json-string to Python objects """
    return json.loads(json_string, object_hook=_translate_json_objects)


def deserialize(data_manipulation_string: str):
    """ Deserialize the data manipulation to configuration objects """
    return _load_from_json(data_manipulation_string)


# #############################################################################
# Serialization Code
# #############################################################################
class _ReplayManipulationEncoder(json.JSONEncoder):
    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            obj_dict = {
                '_cls_': obj.__class__.__name__
            }
            for f in dataclasses.fields(obj):
                obj_dict[f.name] = self.encode(getattr(obj, f.name))
            return obj_dict
        return super().default(obj)


def serialize(manipulation_units):
    return json.dumps(
        obj=manipulation_units,
        cls=_ReplayManipulationEncoder,
    )
