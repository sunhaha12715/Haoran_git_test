#!/usr/bin/env python3.9
import argparse
import json
import pathlib
import re
import xml.etree.ElementTree as ET


API_VERSION_PATTERN = r"""
    v?
    (?:
        (?:(?P<epoch>[0-9]+)!)?                           # epoch
        (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
        (?P<pre>                                          # pre-release
            [-_\.]?
            (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
            [-_\.]?
            (?P<pre_n>[0-9]+)?
        )?
        (?P<post>                                         # post release
            (?:-(?P<post_n1>[0-9]+))
            |
            (?:
                [-_\.]?
                (?P<post_l>post|rev|r)
                [-_\.]?
                (?P<post_n2>[0-9]+)?
            )
        )?
        (?P<dev>                                          # dev release
            [-_\.]?
            (?P<dev_l>dev)
            [-_\.]?
            (?P<dev_n>[0-9]+)?
        )?
    )
    (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
"""

_api_version_re = re.compile(
    r'^\s*' + API_VERSION_PATTERN + r'\s*$',
    re.VERBOSE | re.IGNORECASE,
)

DIAGRAM_VERSION_PATTERN = r"""
    ReplayDiagram_PP16699_Ver_          # Version string identifier
                                        # Followed by the three version numbers
    (?P<major>\d+)
    \.
    (?P<minor>\d+)
    \.
    (?P<patch>\d+)
"""

_diagram_version_re = re.compile(DIAGRAM_VERSION_PATTERN, re.VERBOSE)

SDF_VERSION_PATTERN = r"""
    # Look for 'ModelVersion' and match-all non-greedily afterwards.
        ModelVersion.*?
    # Stop matching all at 'value: ['
        value:\s+
        \[\s*
    # Match major, minor and patch (as floats so ignore the .0 part).
            (?P<major>\d+)\.0\s*,\s*
            (?P<minor>\d+)\.0\s*,\s*
            (?P<patch>\d+)\.0\s*
        \]
"""

_sdf_app_version_re = re.compile(SDF_VERSION_PATTERN, re.VERBOSE | re.DOTALL)


###############################################################################
# Version management functions.
###############################################################################
def dissect_version(version_string):
    return _api_version_re.fullmatch(version_string)


def get_diagram_version(diagram_file):
    diagram_file = pathlib.Path(diagram_file)

    with diagram_file.open() as fhandle:
        root = ET.fromstring(fhandle.read())

    xmlns = '{http://schemas.intempora.com/RTMaps/2011/RTMapsFiles}'
    if root.tag != f'{xmlns}RTMapsDiagram':
        raise Exception(
            f'{diagram_file} does not seem to be a valid diagram: {root.tag}'
        )

    description = root.find(f'./{xmlns}RTBoardView/{xmlns}Description')
    if description is None:
        raise Exception(f'No version available in {diagram_file}')

    m = _diagram_version_re.match(description.text)
    if not m:
        raise Exception(f'No version available in {diagram_file}')
    return int(m.group('major')), int(m.group('minor')), int(m.group('patch'))


def get_sdf_version(sdf_file):
    sdf_file = pathlib.Path(sdf_file)
    if not sdf_file.exists():
        raise FileNotFoundError(sdf_file)

    if sdf_file.suffix != '.sdf':
        raise ValueError(f'Non-SDF file provided: {sdf_file}')

    trc_file = sdf_file.with_suffix('.trc')
    if not trc_file.exists():
        raise FileNotFoundError(trc_file)

    with trc_file.open() as fhandle:
        trc_content = fhandle.read()

    m = _sdf_app_version_re.search(trc_content, re.DOTALL)
    if m is None:
        raise RuntimeError(
            f'Failed to read version from {trc_file}'
        )

    return int(m.group('major')), int(m.group('minor')), int(m.group('patch'))


def get_api_version():
    from dspace.bosch_hol_sdk import __version__ as api_version
    return api_version


###############################################################################
# Command line interface.
###############################################################################
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--diagram',
        help='the RTMaps diagram file (rtd) to be checked',
        type=pathlib.Path,
        default=None,
    )
    parser.add_argument(
        '--sdf',
        help='the SDF file to be checked',
        type=pathlib.Path,
        default=None,
    )
    parser.add_argument(
        '--api',
        help=(
            'the full API version '
            '(implicitly printed if no options are provided)'
        ),
        action='store_true',
    )

    args = parser.parse_args(argv)

    if args.diagram:
        try:
            version = get_diagram_version(args.diagram)
        except Exception as exc:
            print(f'Failed to read diagram version: {exc}')
        else:
            print(json.dumps(version))

    if args.sdf:
        try:
            version = get_sdf_version(args.sdf)
        except Exception as exc:
            print(f'Failed to read SDF version: {exc}')
        else:
            print(json.dumps(version))

    if args.api or (not args.diagram and not args.sdf):
        try:
            print(get_api_version())
        except Exception as exc:
            print(f'Failed to read API version: {exc}')


if __name__ == '__main__':
    main()
