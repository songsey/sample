from __future__ import absolute_import
from __future__ import print_function

from models import common_utils
import numpy as np
import os


def load_data(reader, discretizer, normalizer, small_part=False, return_names=False):
    N = reader.get_number_of_examples()
    if small_part:
        N = 1000
    ret = common_utils.read_chunk(reader, N)
    data = ret["X"]
    ts = ret["t"]
    labels = ret["y"]
    names = ret["name"]
    data = [discretizer.transform(X, end=t)[0] for (X, t) in zip(data, ts)]
    if normalizer is not None:
        data = [normalizer.transform(X) for X in data]
    whole_data = (np.array(data), labels)
    if not return_names:
        return whole_data
    return {"data": whole_data, "names": names}


def save_results(names, pred, y_true, path):
    common_utils.create_directory(os.path.dirname(path))
    with open(path, 'w') as f:
        f.write("stay,prediction,y_true\n")
        for (name, x, y) in zip(names, pred, y_true):
            f.write("{},{:.6f},{}\n".format(name, x, y))

def split_train_val(dataset_dir=None):
    val_patients = set()
    with open(os.path.join(dataset_dir, 'train/listfile.csv')) as listfile:
        for line in listfile:            
            x, y = line.split(',')
            import random 
            if  int(random.uniform(0, 1)<0.2) == 1:
                val_patients.add(x)

        lines = listfile.readlines()
        header = lines[0]
        lines = lines[1:]

    train_lines = [x for x in lines if x[:x.find("_")] not in val_patients]
    val_lines = [x for x in lines if x[:x.find("_")] in val_patients]
    assert len(train_lines) + len(val_lines) == len(lines)

    with open(os.path.join(dataset_dir, 'train_listfile.csv'), 'w') as train_listfile:
        train_listfile.write(header)
        for line in train_lines:
            train_listfile.write(line)

    with open(os.path.join(dataset_dir, 'val_listfile.csv'), 'w') as val_listfile:
        val_listfile.write(header)
        for line in val_lines:
            val_listfile.write(line)

