

class ReplayConfigErrorBase(Exception):
    pass


class ReplayManipulationWithoutStepsError(ReplayConfigErrorBase):
    def __init__(self, unit):
        self._unit = unit

    def __str__(self):
        return f'The data manipulation unit <{self._unit}> has no steps.'


class UnsupportedManipulationTypeError(ReplayConfigErrorBase):
    def __init__(self, unit, support_list):
        self._unit = unit
        self._support_list = support_list

    def __str__(self):
        return (
            f"Data manipulation of type '{self._unit.__class__.__name__}' "
            f"does not support type 'self._unit.type'. Supported: "
            f'{self._support_list}.'
        )
