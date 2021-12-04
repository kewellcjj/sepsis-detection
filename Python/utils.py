import pandas as pd
import numpy as np
from sklearn import metrics
import matplotlib.pyplot as plt

def classification_metrics(classifierName, y_true, y_pred):
    if isinstance(y_true, list):
        y_true = np.array(y_true)
    if isinstance(y_pred, list):
        y_pred = np.array(y_pred)
    y_pred_label = np.greater(y_pred, .5)*1
    accuracy = metrics.accuracy_score(y_true, y_pred_label)
    auc = metrics.roc_auc_score(y_true, y_pred)
    precision = metrics.precision_score(y_true, y_pred_label)
    recall = metrics.recall_score(y_true, y_pred_label)
    f1 = metrics.f1_score(y_true, y_pred_label)

    fpr, tpr, _ = metrics.roc_curve(y_true, y_pred)
    pd.DataFrame({'fpr': fpr, 'tpr': tpr}).to_csv(f"output/{classifierName}_roc.csv", index=False)

    precision_c, recall_c, _ = metrics.precision_recall_curve(y_true, y_pred)
    auprc = metrics.auc(recall_c, precision_c)
    pd.DataFrame({'recall': recall_c, 'precision': precision_c}).to_csv(f"output/{classifierName}_prc.csv", index=False)

    print("______________________________________________")
    print(("Classifier: "+classifierName))
    print((f"Accuracy: {accuracy: .4f}"))
    print((f"AUC: {auc: .4f}"))
    print((f"AUPRC: {auprc: .4f}"))
    print((f"Precision: {precision: .4f}"))
    print((f"Recall: {recall: .4f}"))
    print((f"F1-score: {f1: .4f}"))
    print("______________________________________________")
    print("")

def plot_roc(figname, **kwargs):
    with plt.style.context('ieee'):
        plt.figure()
        for classifierName, rocfile in kwargs.items():
            roc = pd.read_csv(rocfile)
            auc = metrics.auc(roc.fpr, roc.tpr)
            plt.plot(roc.fpr, roc.tpr,label=f"{classifierName} AUC={auc:.3f}")
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.legend()
        plt.savefig(figname)

def plot_prc(figname, **kwargs):
    with plt.style.context('ieee'):
        plt.figure()
        for classifierName, prcfile in kwargs.items():
            prc = pd.read_csv(prcfile)
            auprc = metrics.auc(prc.recall, prc.precision)
            plt.plot(prc.recall, prc.precision,label=f"{classifierName} AUPRC={auprc:.3f}")
        plt.xlabel("Recall")
        plt.ylabel("Precision")
        plt.legend()
        plt.savefig(figname)