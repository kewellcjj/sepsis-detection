# borrowed gatech cse6250 hw5 template

import os
import torch
from torch import dropout
import torch.nn as nn
from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import confusion_matrix, roc_auc_score

class RNN(nn.Module):
    def __init__(self, dim_input):
        super(RNN, self).__init__()

        self.gru = nn.GRU(dim_input, 128, 3, batch_first=True, dropout=.5)
        self.fc1 = nn.Linear(128, 2)
#         self.bn = nn.BatchNorm1d(64)
#         self.relu = nn.ReLU()
#         self.dropout = nn.Dropout(.2)
#         self.fc2 = nn.Linear(64, 2)


    def forward(self, input_tuple):
        seqs, lengths = input_tuple

        out = pack_padded_sequence(input=seqs, batch_first=True, lengths=lengths.cpu())
    
        _, out = self.gru(out)
        out = self.fc1(out[-1,:,:].squeeze())
#         out = self.fc2(self.dropout(self.relu(self.bn(out))))
        return out

class AverageMeter(object):
    """Computes and stores the average and current value"""

    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count

def train(model, device, data_loader, criterion, optimizer, epoch):
    losses = AverageMeter()

    model.train()

    results = []
    for i, (input, target) in enumerate(data_loader):

        if isinstance(input, tuple):
            input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
        else:
            input = input.to(device)

        target = target.to(device)

        optimizer.zero_grad()
        output = model(input)
        loss = criterion(output, target)
        assert not np.isnan(loss.item()), 'Model diverged with loss = NaN'

        loss.backward()
        optimizer.step()
        
        losses.update(loss.item(), target.size(0))
        
        y_true = target.detach().to('cpu').numpy().tolist()
        y_pred = nn.Softmax(1)(output).detach().to('cpu').numpy()[:,1].tolist()
        results.extend(list(zip(y_true, y_pred)))

    y_true, y_pred = zip(*results)
    auc = roc_auc_score(y_true, y_pred)

    return losses.avg, auc


def evaluate(model, device, data_loader, criterion):
    losses = AverageMeter()

    results = []

    model.eval()

    with torch.no_grad():
        for i, (input, target) in enumerate(data_loader):

            if isinstance(input, tuple):
                input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
            else:
                input = input.to(device)
            target = target.to(device)

            output = model(input)
            loss = criterion(output, target)

            losses.update(loss.item(), target.size(0))

            y_true = target.detach().to('cpu').numpy().tolist()
            y_pred = nn.Softmax(1)(output).detach().to('cpu').numpy()[:,1].tolist()
            results.extend(list(zip(y_true, y_pred)))
        
        y_true, y_pred = zip(*results)
        auc = roc_auc_score(y_true, y_pred)

    return losses.avg, auc

def plot_learning_curves(train_losses, valid_losses, train_aucs, valid_aucs):
    epoch = len(train_losses)
    plt.figure()
    plt.plot(range(epoch), train_losses, label='Train')
    plt.plot(range(epoch), valid_losses, label='Validation')
    plt.ylabel('Loss')
    plt.xlabel('epoch')
    plt.legend(loc="best")
    plt.savefig('./output/rnn_loss.png')

    plt.figure()
    plt.plot(range(epoch), train_aucs, label='Train')
    plt.plot(range(epoch), valid_aucs, label='Validation')
    plt.ylabel('AUC')
    plt.xlabel('epoch')
    plt.legend(loc="best")
    plt.savefig('./output/rnn_auc.png')