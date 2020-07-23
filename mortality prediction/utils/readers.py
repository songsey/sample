from __future__ import absolute_import
from __future__ import print_function

import os
import numpy as np
import random
import boto3

class Reader(object):
    def __init__(self, dataset_dir, listfile=None):
        self._dataset_dir = dataset_dir
        self._current_index = 0
        if listfile is None:
            listfile_path = os.path.join(dataset_dir, "listfile.csv")
        else:
            listfile_path = listfile
      
        s3 = boto3.client('s3')

        response = s3.get_object(Bucket='aws-glue-scripts-271538242229-us-west-2', Key=listfile_path)
        x=list(response['Body'].iter_lines())
        self._data=[x.decode('utf8').strip() for x in x]   
        self._listfile_header = self._data[0]
        #print(self._listfile_header) 
        self._data = self._data[1:]

    def get_number_of_examples(self):
        return len(self._data)

    def random_shuffle(self, seed=None):
        if seed is not None:
            random.seed(seed)
        random.shuffle(self._data)

    def read_example(self, index):
        raise NotImplementedError()

    def read_next(self):
        to_read_index = self._current_index
        self._current_index += 1
        if self._current_index == self.get_number_of_examples():
            self._current_index = 0
        return self.read_example(to_read_index)



class read_ts(Reader):
    def __init__(self, dataset_dir, listfile=None, period_length=48.0):
        Reader.__init__(self, dataset_dir, listfile)

        self._data = [line.split(',') for line in self._data]
        
        self._data = [(x, int(y)) for (x, y) in self._data]
        self._period_length = period_length

    def _read_timeseries(self, ts_filename):
        ret = []
        s3 = boto3.client('s3')
        
        key=self._dataset_dir+ts_filename
        #print(key)
        
        response = s3.get_object(Bucket='aws-glue-scripts-271538242229-us-west-2', Key=key)
        
        x=list(response['Body'].iter_lines())
        tsfile=[x.decode('utf8').strip() for x in x]   
        header= tsfile[0].strip().split(',')
        #print(header)
        assert header[0] == "Hours"
        for line in tsfile[1:]:
                mas = line.strip().split(',')
                ret.append(np.array(mas))
   
        return (np.stack(ret), header)

    def read_example(self, index):
        if index < 0 or index >= len(self._data):
            raise ValueError("Index must be from 0 (inclusive) to number of lines (exclusive).")

        name = self._data[index][0]
        t = self._period_length
        y = self._data[index][1]
        (X, header) = self._read_timeseries(name)

        return {"X": X,
                "t": t,
                "y": y,
                "header": header,
                "name": name}

