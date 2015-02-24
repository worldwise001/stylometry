import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc


def generate(filename, rows, columns, title=None):
    rows_num = range(1, len(rows)+1)
    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)
    plt.scatter(rows_num, columns)
    locs, labels = plt.xticks(rows_num, rows)
    plt.setp(labels, rotation=90)
    plt.plot(rows_num, columns)
    plt.title(title)
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')


def roc(filename, y_truth, y_predicted, title=None):
    fpr, tpr, _ = roc_curve(y_truth, y_predicted, 1)
    roc_auc = auc(fpr, tpr)

    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)
    plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0,1], [0,1], 'k--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC: %s' % title)
    plt.legend(loc="lower right")
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')


def rocs(filename, y_truths, y_predicteds, labels, title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)

    for i in range(0, len(y_truths)):
        fpr, tpr, _ = roc_curve(y_truths[i], y_predicteds[i], 1)
        roc_auc = auc(fpr, tpr)
        plt.plot(fpr, tpr, label='%s (area = %0.2f)' % (labels[i], roc_auc))

    plt.plot([0,1], [0,1], 'k--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC: %s' % title)
    plt.legend(loc="lower right")
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')
