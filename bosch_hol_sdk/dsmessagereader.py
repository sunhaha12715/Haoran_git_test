"""
A class to read the messages from a dSPACE SMART system.

@copyright
    Copyright 2024, dSPACE Mechatronic Control Technology (Shanghai) Co., Ltd.
    All rights reserved.
"""
import dataclasses
import enum
import logging
import requests


class DsMessageType(str, enum.Enum):
    DEBUG = 'Log'
    LOG = DEBUG
    INFO = 'Info'
    WARNING = 'Warning'
    ERROR = 'Error'

    @property
    def logging_level(self):
        try:
            return getattr(logging, self.name)
        except AttributeError:
            logging.exception('Failed to get logging level')
            return logging.ERROR


@dataclasses.dataclass(frozen=True)
class DsMessage:
    type: DsMessageType
    node: int
    source: str
    time: str
    hwtime: str
    application_id: int
    errorcode: str
    message: str

    @classmethod
    def from_dict(cls, d):
        try:
            _type = DsMessageType(d['type'])
        except ValueError:
            logging.exception(d)
            _type = DsMessageType.ERROR

        return cls(
            type=DsMessageType(_type),
            node=d['node'],
            source=d['source'],
            time=d['time'],
            hwtime=d['hwtime'],
            application_id=int(d['application_id'], 16),
            errorcode=d['errorcode'],
            message=d['message'],
        )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'type: {self.type}; node: {self.node}; source: {self.source}; '
            f'time: {self.time}; application: {hex(self.application_id)}; '
            f'errorcode: {self.errorcode}: {self.message}'
            ')'
        )


class DsMessagesReader(object):
    def __init__(self, system, skip_available=True, logger=None):
        logger = logger or logging.getLogger()
        ip_str = system.ip.replace('.', '_')
        self._logger = logger.getChild(f'{self.__class__.__name__}.{ip_str}')
        self._system = system
        self._url = f'http://{system.ip}/api/v1.0/messages?'
        self._boot_uuid = 0
        self._next_msg_id = 0

        if skip_available:
            self.get_messages()

    def _request_messages(self, timeout=0.1, **kwargs):
        args_str = '&'.join(f'{k}={v}' for k, v in kwargs.items())
        resp = requests.get(f'{self._url}{args_str}', timeout=timeout)
        return resp.json()

    def _convert_messages(self, messages):
        return [DsMessage.from_dict(m) for m in messages]

    def get_messages(self, count=-1, warn_on_reboot=True):
        # Execute the GET request.
        try:
            resp = self._request_messages(
                boot_uuid=self._boot_uuid,
                next_msg_id=self._next_msg_id,
                max_count=count,
            )
        except requests.exceptions.ConnectTimeout as exc:
            self._logger.warning(f'{self._system} not reachable: {exc}')
            return []
        boot_uuid = resp['boot_uuid']
        next_msg_id = resp['next_msg_id']
        messages = self._convert_messages(resp['messages'])

        self._logger.debug(
            f'Received {len(messages)} (requested {count})'
        )

        # Reboot check.
        if (
                warn_on_reboot
                and self._boot_uuid != 0
                and self._boot_uuid != boot_uuid
        ):
            self._logger.warning(
                f'{self._system} seems to have been restarted!'
            )

        # Update the current value
        self._boot_uuid = boot_uuid
        self._next_msg_id = next_msg_id
        # return the messages.
        return messages
