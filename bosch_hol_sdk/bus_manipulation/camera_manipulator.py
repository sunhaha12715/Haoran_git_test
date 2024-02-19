from .manipulationbase import RTMapsManipulationBase
from dspace.bosch_hol_sdk.configuration import ReplayManipulationType


class CameraManipulator(RTMapsManipulationBase):
    def apply(self):
        """ Apply the Camera manipulation object. """
        config = self.configuration

        if config.type == ReplayManipulationType.SUSPEND:
            self._logger.debug(
                f'Applying suspend to {config.name} at {config.steps}'
            )
            component_name = f'frame_skipper_{config.name}'
            start_time = ','.join(map(
                lambda x: str(x.start),
                config.steps,
            ))
            duration = ','.join(map(
                lambda x: str(x.duration),
                config.steps,
            ))
            self._logger.debug(
                f'Configuring component {component_name} with "{start_time}" '
                f'and "{duration}"'
            )
            try:
                self._rtmaps.set_property(
                    component_name,
                    'start_time',
                    start_time,
                )
                self._rtmaps.set_property(
                    component_name,
                    'duration',
                    duration,
                )
            except Exception:
                self._logger.exception(
                    f'Failed to set the properties of {component_name}!'
                )
                raise
        else:
            # Should never happen because we check on (de)serialization.
            self._logger.error(
                f'Unsupported manipulation type: {config.type}'
            )
