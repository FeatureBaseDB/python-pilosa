from codecs import open
from os import path

from setuptools import setup

here = path.abspath(path.dirname(__file__))

try:
    import pypandoc
    long_description = pypandoc.convert_file("README.md", "rst")
except ImportError:
    # long_description is required only during the release to PyPI
    long_description = u''

exec(open(path.join(here, 'pilosa/version.py'), 'r').read())

setup(
    name='pilosa',
    version=get_version_setup(),
    description='Python client library for Pilosa',
    long_description=long_description,
    url='https://github.com/pilosa/python-pilosa',
    author='Pilosa Engineering',
    author_email='dev@pilosa.com',
    license='BSD',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: BSD License',
    ],

    keywords='pilosa,pql',
    packages=['pilosa'],

    setup_requires=['pytest-runner'],
    install_requires=['urllib3', 'protobuf'],
    tests_require=['pytest', 'mock', 'coverage', 'pytest-cov'],
)
