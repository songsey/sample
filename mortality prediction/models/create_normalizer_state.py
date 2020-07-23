from __future__ import absolute_import
from __future__ import print_function

from utils.readers import read_ts
from models.preprocessing import Discretizer, Normalizer

import os

def create_normalizer(timestep=1.0, impute_strategy='previous',start_time='zero',store_masks=True, no_masks=None, n_samples=-1, output_dir='.', data=None ,dataset_dir=None, listfile=None):

    # create the reader
    reader = None
    #dataset_dir = 'data/in-hospital-mortality/train/' # os.path.join(data, 'train')
    reader = read_ts(dataset_dir=dataset_dir, listfile=listfile, period_length=48.0)
    # create the discretizer
    discretizer = Discretizer(timestep=timestep,
                              store_masks=store_masks,
                              impute_strategy=impute_strategy,
                              start_time=start_time)
    discretizer_header = reader.read_example(0)['header']
    continuous_channels = [i for (i, x) in enumerate(discretizer_header) if x.find("->") == -1]

    # create the normalizer
    normalizer = Normalizer(fields=continuous_channels)

    # read all examples and store the state of the normalizer
    n_samples = n_samples
    if n_samples == -1:
        n_samples = reader.get_number_of_examples()

    for i in range(n_samples):
        if i % 1000 == 0:
            print('Processed {} / {} samples'.format(i, n_samples), end='\r')
        ret = reader.read_example(i)
        data, new_header = discretizer.transform(ret['X'], end=ret['t'])
        normalizer._feed_data(data)
    print('\n')
    #'{}_ts:{:.2f}_impute:{}_start:{}_masks:{}_n:{}.normalizer'
    file_name = 'ts{:.2f}_impute{}_start{}_masks{}_n{}.normalizer'.format(
        timestep, impute_strategy, start_time, store_masks, n_samples)
    file_name = os.path.join(output_dir, file_name)
    print('Saving the state in {} ...'.format(file_name))
    normalizer._save_params(file_name)
