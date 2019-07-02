# Pilosa Multi-field CSV Import

## Prerequisites

* Python 3.6 or better

## Install

Create a virtual environment:

    $ python3 -m venv ve

Activate the virtual environment:

    $ source ve/bin/activate

Install requirements:

    $ pip install -r requirements.txt

## Usage

* Update `import.py` so it matches the contents of the CSV file,
* Run it  with the Pilosa address (by default: `localhost:10101`) and name of the CSV file:

    $ python import.py :10101 sample.csv
