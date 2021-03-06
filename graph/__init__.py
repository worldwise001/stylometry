import matplotlib
matplotlib.use('Agg')
import statsmodels.api as sm
import statsmodels.formula.api as smf
import numpy as np
from scipy.stats import linregress
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc


def hist_prebin(filename, values, width=1, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    left = [ v[0] for v in values ]
    height = [ v[1] for v in values ]

    plt.figure(figsize=(24,18), dpi=600)
    plt.bar(left=left, height=height, width=width)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.title(title)
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')


def hist(filename, values, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)
    plt.hist(values, bins=20)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.title(title)
    plt.savefig('%s.png' % filename, format='png')
    plt.savefig('%s.eps' % filename, format='eps')


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


def scatter(filename, x, y, line=True, xr=None, yr=None, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(24,18), dpi=600)
    plt.scatter(x, y)

    if xr is not None:
        plt.xlim(xr)
    if yr is not None:
        plt.ylim(yr)

    if line:
        est = sm.OLS(y, sm.add_constant(x)).fit()
        x_prime = np.linspace(min(x), max(x), 100)[:, np.newaxis]
        x_prime = sm.add_constant(x_prime)
        y_hat = est.predict(x_prime)
        line_plot1 = plt.plot(x_prime[:, 1], y_hat, 'r', alpha=0.9, label='r^2 = %s' % est.rsquared)
        #res = linregress(x,y)
        #line_plot2 = plt.plot([min(x), max(x)], [res[0]*min(x)+res[1], res[0]*max(x)+res[1]],
        #                      'g', alpha=0.9, label='r^2 = %s' % res[2])
        plt.legend(['r^2 = %s' % est.rsquared])

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

def boxplot_single(filename, data, xr=None, yr=None, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    author_labels = []
    author_data = []
    for author in data:
        author_labels.append(author)
        author_data.append(data[author])

    for start in range(0, len(data), 50):
        end = start+50
        if end > len(data):
            end = len(data)
        width = end-start
        fig = plt.figure(figsize=(width,12), dpi=600)
        ax = plt.axes()

        bp = plt.boxplot(author_data[start:end], positions=range(1, width+1), widths = 0.8)
        plt.xlim(0, width+1)
        ax.set_xticklabels(author_labels[start:end], rotation=70)
        ax.set_xticks(range(1, width+1))
        if xr is not None:
            plt.xlim(xr)
        if yr is not None:
            plt.ylim(yr)
        plt.xlabel(x_title)
        plt.ylabel(y_title)
        plt.title(title)
        plt.tight_layout()

        plt.savefig('%s_%d.png' % (filename,start), format='png')
        plt.savefig('%s_%d.eps' % (filename,start), format='eps')
        plt.close()

def boxplot(filename, data, groups, x_title='', y_title='', title=None):
    if title is None:
        title = filename

    plt.figure(figsize=(1.5*len(data)+3,12), dpi=600)
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
    plt.tight_layout()

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

