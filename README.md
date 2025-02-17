[![Build Status](https://travis.ibm.com/gbs-mondelez-garage/covid_impact.svg?token=2ZdBCMyxNNWyU7qr4sC6&branch=master)](https://travis.ibm.com/gbs-mondelez-garage/covid_impact) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

covid_impact
==============================

CPG Covid Impact Analysis and Projections for Mondelez

Getting Started
------------
This project assumes you have Anaconda or Miniconda installed on your machine. If you do not, please install from https://docs.conda.io/en/latest/miniconda.html

1. `git clone` this repo in the desired directory on your local machine.
2. `cd` into the project directory
3. Run `conda env update && conda activate mdlz_covid`
4. Run `pip install -e .`
5. Run `pre-commit install`

When revisiting the codebase, make sure you activate the enironment by running `conda activate mdlz_covid`. If you fail to do this, you may run into ImportErrors and could potentially develop code that does not work with the rest of the project dependencies.

Project Organization
------------

    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── environment.yml    <- The enironment file for reproducing the analysis environment, e.g.
    │                         generated with `conda env export --no-builds | grep -v "prefix" > environment.yml`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so covid_impact can be imported
    └── covid_impact       <- Source code for use in this project.
        ├── __init__.py    <- Makes src a Python module
        │
        ├── data_prep      <- Scripts to download or generate data
        │   └── make_dataset.py
        │
        ├── feat_eng       <- Scripts to turn raw data into features for modeling
        │   └── build_features.py
        │
        ├── models         <- Scripts to train models and then use trained models to make
        │   │                 predictions
        │   ├── predict_model.py
        │   └── train_model.py
        │
        └── visualization  <- Scripts to create exploratory and results oriented visualizations
            └── visualize.py
