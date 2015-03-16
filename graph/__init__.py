import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc


def generate(filename, rows, columns, x_title='', y_title='', title=None):
    rows_num = range(1, len(rows)+1)
    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)
    plt.scatter(rows_num, columns)
    locs, labels = plt.xticks(rows_num, rows)
    plt.setp(labels, rotation=90)
    plt.plot(rows_num, columns)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.title(title)
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')


def scatter(filename, x, y, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)
    plt.scatter(x, y)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.title(title)
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')
    plt.close()


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

def setboxcol(bp, i, col):
    plt.setp(bp['boxes'][i], color=col)
    plt.setp(bp['caps'][i*2], color=col)
    plt.setp(bp['caps'][i*2+1], color=col)
    plt.setp(bp['whiskers'][i*2], color=col)
    plt.setp(bp['whiskers'][i*2+1], color=col)
    plt.setp(bp['fliers'][i*2], color=col)
    plt.setp(bp['fliers'][i*2+1], color=col)
    plt.setp(bp['medians'][i], color=col)

def boxplot_single(filename, data, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(0.8*len(data),12), dpi=600)
    ax = plt.axes()

    author_labels = []
    author_data = []

    for author in data:
        author_labels.append(author)
        author_data.append(data[author])

    bp = plt.boxplot(author_data, positions=range(1, len(data)+1), widths = 0.8)
    plt.xlim(0, len(data)+1)
    ax.set_xticklabels(author_labels, rotation=70)
    ax.set_xticks(range(1, len(data)+1))
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.title(title)

    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')
    plt.close()

def boxplot(filename, data, groups, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(1.5*len(data),12), dpi=600)
    ax = plt.axes()

    colors=['blue', 'red', 'green']*10

    i = 1
    k = 0
    interval = len(groups)
    print(groups)
    author_labels = []
    author_label_pos = []

    for author in data:
        author_labels.append(author)
        author_data = []
        if interval == 0:
            interval = len(data[author])
        cols = []
        for src_reddit in data[author]:
            author_data.append(data[author][src_reddit])
            print(groups.index(src_reddit))
            cols.append(colors[groups.index(src_reddit)])
        pos = [ i+j for j in range(0, interval) ]
        bp = plt.boxplot(author_data, positions=pos, widths = 0.8)
        for m in range(0, interval):
            setboxcol(bp, m, cols[m])
        author_label_pos.append(i + (interval/2.0))
        i += interval + 1
        k += 1

    plt.xlim(0, i)
    ax.set_xticklabels(author_labels, rotation=70)
    ax.set_xticks(author_label_pos)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.title(title)

    hB, = plt.plot([1,1],'b-')
    hR, = plt.plot([1,1],'r-')
    hG, = plt.plot([1,1],'g-')
    plt.legend((hB, hR, hG),(groups[0], groups[1], groups[2]))
    hB.set_visible(False)
    hR.set_visible(False)
    hG.set_visible(False)

    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')
    plt.close()

