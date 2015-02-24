# stylometry
Stylometric (parallel) framework in Python for big data in clusters

## Features
* Parallelized (thus fast)
* Intended to integrate with a database-based corpus
* A variety of feature-generation techniques:
  * byte-ngrams
  * word-ngrams
  * readability metrics
  * simple statistics
  * part-of-speech tagging
  * part-of-speech ngrams
  * word/pos hybrids
* Plugs into a variety of stylometric techniques:
  * ppm-c (compression)
  * dmc (compression)
  * gvc (spam-filter)
  * sofia-ml (machine learning)
* Some graphing utilities to show performance

We also provide some plugs to transform existing corpora into database format.
We also provide some plugs to export features into SVM-light sparse data format.

## Assumptions
We assume you have lots of RAM or lots of time or lots of CPU cores or all 3.

## Haphazard off-the-cuff observed metrics
* 30 million comments generally takes about a day to process 1 type of feature
* 3 million posts generally takes about an hour to process 1 type of feature
