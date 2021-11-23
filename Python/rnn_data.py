import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset

def etl_rnn(path):
    case_static = pd.read_csv(f"{path}/static_variables_cases_ex3h.csv").drop(columns=['subject_id'])
    case_static['sepsis'] = 1
    case_lab = pd.read_csv(f"{path}/case_48h_labs_ex3h.csv").drop(columns=['subject_id', 'chart_time', 'sepsis_onset'])
    case_lab['hr_feature'] = 45-np.ceil(case_lab['hr_feature'])
    case_lab = case_lab.groupby(['icustay_id', 'hr_feature'], as_index=False).mean()
    case_vital = pd.read_csv(f"{path}/case_48h_vitals_ex3h.csv").drop(columns=['subject_id'])
    case_vital['hr_feature'] = 45-np.ceil(case_vital['hr_feature'])
    case_vital = case_vital.groupby(['icustay_id', 'hr_feature'], as_index=False).mean()
    case_lab_vital = case_lab.merge(case_vital, on=['icustay_id', 'hr_feature'], how='outer')

    control_static = pd.read_csv(f"{path}/static_variables_controls_ex3h.csv").drop(columns=['subject_id']) \
        .rename(columns={'control_onset_time': 'sepsis_onset', 'control_onset_hour': 'sepsis_onset_hour'})
    control_static['sepsis'] = 0
    control_lab = pd.read_csv(f"{path}/control_48h_labs_ex3h.csv").drop(columns=['subject_id', 'chart_time', 'control_onset_time'])
    control_lab['hr_feature'] = 45-np.ceil(control_lab['hr_feature'])
    control_lab = control_lab.groupby(['icustay_id', 'hr_feature'], as_index=False).mean()
    control_vital = pd.read_csv(f"{path}/control_48h_vitals_ex3h.csv").drop(columns=['subject_id'])
    control_vital['hr_feature'] = 45-np.ceil(control_vital['hr_feature'])
    control_vital = control_vital.groupby(['icustay_id', 'hr_feature'], as_index=False).mean()
    control_lab_vital = control_lab.merge(control_vital, on=['icustay_id', 'hr_feature'], how='outer')

    sequence_all = pd.concat([case_lab_vital, control_lab_vital]).set_index(['icustay_id', 'hr_feature']).sort_index()
    sequence_all = sequence_all.groupby(level=0).ffill().fillna(0)
#     sequence_all = sequence_all.fillna(0)
    sequence_all = sequence_all.reset_index(level=1)
    
    case_static = case_static.merge(case_lab[['icustay_id']].drop_duplicates('icustay_id'), on='icustay_id').merge(case_vital[['icustay_id']].drop_duplicates('icustay_id'), on='icustay_id')
    control_static = control_static.merge(control_lab[['icustay_id']].drop_duplicates('icustay_id'), on='icustay_id').merge(control_vital[['icustay_id']].drop_duplicates('icustay_id'), on='icustay_id')
    static_all = pd.concat([case_static, control_static]).set_index('icustay_id')
    static_all = static_all.join(sequence_all.groupby(sequence_all.index).first(), how='inner')
    
    return static_all, sequence_all

# borrowed gatech cse6250 hw5 template
class VisitSequenceWithLabelDataset(Dataset):
    def __init__(self, seqs, labels):
        self._labels = labels.values.squeeze()
        self._seqs = []
        for i in labels.index:
            self._seqs.append(seqs[seqs.index==i].values)

    def __len__(self):
        return len(self._labels)

    def __getitem__(self, idx):
        return self._seqs[idx], self._labels[idx]

# borrowed gatech cse6250 hw5 template
def seq_collate_fn(batch):
    """
    DataLoaderIter call - self.collate_fn([self.dataset[i] for i in indices])
    Thus, 'batch' is a list [(seq_1, label_1), (seq_2, label_2), ... , (seq_N, label_N)]
    where N is minibatch size, seq_i is a Numpy (or Scipy Sparse) array, and label is an int value

    :returns
        seqs (FloatTensor) - 3D of batch_size X max_length X num_features
        lengths (LongTensor) - 1D of batch_size
        labels (LongTensor) - 1D of batch_size
    """

    tmp = []
        
    max_length = max([s[0].shape[0] for s in batch])
    
    for seq, label in batch:
        tmp.append((np.vstack([seq, np.zeros((max_length-seq.shape[0], seq.shape[1]))]), seq.shape[0], label))

    tmp.sort(key=lambda x: x[1], reverse=True)


    seqs_tensor = torch.FloatTensor(np.array([t[0] for t in tmp]))
    lengths_tensor = torch.LongTensor(np.array([t[1] for t in tmp]))
    labels_tensor = torch.LongTensor(np.array([t[2] for t in tmp]))

    return (seqs_tensor, lengths_tensor), labels_tensor