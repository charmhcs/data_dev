import pathlib
import pkg_resources
from setuptools import setup

with pathlib.Path('requirements.txt').open() as requirements_txt:
    reqs = [
        str(requirement)
        for requirement
        in pkg_resources.parse_requirements(requirements_txt)
    ]

setup(
    name='etl-common',
    version='0.1',
    install_requires=reqs,
    package_dir={'': 'src'},
    packages=['helpers.abstract',
              'helpers.constant',
              'helpers.factory',
              'helpers.schema',
              'config',
              ],
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7'
    ]
)