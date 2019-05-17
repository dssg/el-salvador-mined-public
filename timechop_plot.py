# coding: utf-8

from triage.component.timechop.plotting import visualize_chops
from triage.component.timechop import Timechop

import yaml

import matplotlib.pyplot as plt
import numpy as np


if __name__ == '__main__':
    with open('simple_config.yaml') as f:
        experiment_config = yaml.load(f)

    chopper = Timechop(**(experiment_config["temporal_config"]))
    visualize_chops(chopper)