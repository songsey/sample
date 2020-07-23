from __future__ import absolute_import
from __future__ import print_function

from keras.models import Model
from keras.layers import Input, Dense, LSTM, GRU, Masking, Dropout
from keras.layers.wrappers import Bidirectional, TimeDistributed
from keras.layers.merge import Concatenate

from models.keras_utils import LastTimestep
from models.keras_utils import ExtendMask
from models.keras_utils import Slice, LastTimestep


class Network(Model):

    def __init__(self, dim, batch_norm, dropout, rec_dropout, target_repl=False, model=None, channel_wise=None, header=None
     ,num_classes=1, depth=2, input_dim=76,  is_bidirectional = True,  **kwargs):

        self.dim = dim
        self.batch_norm = batch_norm
        self.dropout = dropout
        self.rec_dropout = rec_dropout
        self.depth = depth
        self.size_coef= 4 
        self.is_bidirectional=is_bidirectional
        self.model=model

        final_activation = 'sigmoid'
        
        # Input layers and masking
        X = Input(shape=(None, input_dim), name='X')
        inputs = [X]
        mX = Masking()(X)

        # if cw
        if channel_wise:
            channel_names = set()
            for ch in header:
                if ch.find("mask->") != -1:
                    continue
                pos = ch.find("->")
                if pos != -1:
                    channel_names.add(ch[:pos])
                else:
                    channel_names.add(ch)
            channel_names = sorted(list(channel_names))
            print("==> found {} channels: {}".format(len(channel_names), channel_names))

            channels = [] 
            for ch in channel_names:
                indices = range(len(header))
                indices = list(filter(lambda i: header[i].find(ch) != -1, indices))
                channels.append(indices)
            
            cX = []
            for ch in channels:
                cX.append(Slice(ch)(mX)) # preprocess each channel
            
            pX = [] # processed by rnn  
            for x in cX:
                p = x
                for i in range(depth):
                    n=dim
                    if is_bidirectional:
                        n = n // 2 
                        #print (n)
                    if model=='LSTM':
                        mod = LSTM(units=n,
                                activation='tanh',
                                return_sequences=True,
                                dropout=dropout,
                                recurrent_dropout=rec_dropout)
                    else:
                        mod = GRU(units=n,
                            activation='tanh',
                            return_sequences=True,
                             recurrent_dropout=rec_dropout,
                            dropout=dropout)
            
                    if is_bidirectional:
                        p = Bidirectional(mod)(p)
                    else:
                        p = mod(p)
                pX.append(p)

            Z = Concatenate(axis=2)(pX)  # concat processed channel

        for i in range(depth - 1):
            if channel_wise:
                n = int(self.size_coef * dim) #          
                if is_bidirectional:
                    n = n // 2  
            else:
                n= dim
                if is_bidirectional:
                    n = n // 2
         
            if model=='LSTM':
                mod = LSTM(units=n,
                        activation='tanh',
                        return_sequences=True,
                         recurrent_dropout=rec_dropout,
                        dropout=dropout)
            else:
                mod = GRU(units=n,
                        activation='tanh',
                        return_sequences=True,
                         recurrent_dropout=rec_dropout,
                        dropout=dropout)
            
            if channel_wise==False:    
                if is_bidirectional:
                    mX = Bidirectional(mod)(mX)
                else:
                    mX = mod(mX)
            else:
            
                if is_bidirectional:
                    Z = Bidirectional(mod)(Z)
                else:
                    Z= mod(Z)

            
        # Output 
        return_sequences = target_repl
        if channel_wise==False:
            if model=='LSTM':
                m = LSTM(units=dim,activation='tanh', return_sequences=return_sequences, dropout=dropout, recurrent_dropout=rec_dropout)(mX)
            else:
                m = GRU(units=dim,activation='tanh', return_sequences=return_sequences, dropout=dropout, recurrent_dropout=rec_dropout)(mX)
        else:      
            if model=='LSTM':
                m = LSTM(units=int(self.size_coef*dim),activation='tanh', return_sequences=return_sequences, dropout=dropout, recurrent_dropout=rec_dropout)(Z)
            else:
                m = GRU(units=int(self.size_coef*dim),activation='tanh', return_sequences=return_sequences, dropout=dropout, recurrent_dropout=rec_dropout)(Z)
          
        if dropout > 0: m = Dropout(dropout)(m)

        if target_repl:
            y = TimeDistributed(Dense(num_classes, activation=final_activation), name='seq')(m)
            y_last = LastTimestep(name='single')(y)
            outputs = [y_last, y]
        else:
            y = Dense(num_classes, activation=final_activation)(m)
            outputs = [y]

        super(Network, self).__init__(inputs=inputs, outputs=outputs)

    def say_name(self):
        return "{}.n{}{}{}{}.dep{}".format(self.model, self.dim,
                                           ".bn" if self.batch_norm else "",
                                           ".d{}".format(self.dropout) if self.dropout > 0 else "",
                                           ".rd{}".format(self.rec_dropout) if self.rec_dropout > 0 else "",
                                           self.depth)
