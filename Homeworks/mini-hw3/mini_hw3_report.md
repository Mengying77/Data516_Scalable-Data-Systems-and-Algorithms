# CSED 516 Mini-Homework 3 Mengying Bi
## Objectives:

Run queries on [Apache Beam](https://beam.apache.org/).

## Assignment tools

Apache Beam installed on an EC2 node (best), your local machine, or on a Google cloud account.

## What to Turn In

Submit answers to the questions listed below.  

# Assignment Dataset

## Environment 

In this Assignment you will need access to a node running Apache Beam.
You can do this on an EC2 instance (see the [section notes](https://courses.cs.washington.edu/courses/csed516/19au/sections/beam-wordcount.pdf)), install Beam on your local system, or use a Google Cloud account.

## Dataset

  - [hamlet.txt](https://courses.cs.washington.edu/courses/csed516/19au/sections/hamlet.txt)
  - [muchado.txt](https://courses.cs.washington.edu/courses/csed516/19au/sections/muchAdo.txt)

Ingest both of these datasets in Beam system.

## Questions (20 points)

- Count the *total number of words* in both the `hamlet` and `muchado` datasets.
```
Result: 
muchAdo.txt 
Number of total words: 22760

hamlet.txt 
Number of total words: 32036
```

- Count the *average number of letters per word* in both the `hamlet` and `muchado` datasets.
```
Result: 
muchAdo.txt 
Average word length: 4.083040421792619

hamlet.txt 
Average word length: 4.084685978274441
```

Submit the *time* required to generate the results for each question and dataset, as well as the results you obtained.  Also indicate whether you used an EC2 instance (if so, indicate the instance type), your local machine, or a Google Cloud account.

```
Running on local machine. 
muchAdo.txt 
Pipeline Execution time =  1.4645471572875977
Word length runtime =  0.0006489753723144531
Total words runtime =  0.0002639293670654297

hamlet.txt 
Pipeline Execution time =  1.8088366985321045
Word length runtime =  0.0007219314575195312
Total words runtime =  0.00030493736267089844

CPU times and Wall time recorded below.
```

```python
#set up the environment
!pip install apache_beam 

import logging
logging.getLogger().setLevel(logging.ERROR)
logging.basicConfig()

import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions


from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter

import time

input_file = "muchAdo.txt"
input_file2 = "hamlet.txt"
output_file = "simple_counts"

class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self):
        super(WordExtractingDoFn, self).__init__()
        self.word_counter = Metrics.counter(self.__class__, 'num_words')
        self.word_lengths_dist = Metrics.distribution(self.__class__, 'word_len_dist')
        
    def process(self, line):
        text_line = line.strip()
        words = re.findall(r'[A-Za-z\']+', text_line)
        for word in words:
            self.word_counter.inc()
            self.word_lengths_dist.update(len(word))
        return words
```
```python
%time
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DirectRunner'
#Set pipeline options
p = beam.Pipeline(options=options)

lines = p | 'read' >> ReadFromText(input_file)

counts = (lines
          | "split" >> (beam.ParDo(WordExtractingDoFn()))
                        #.with_output_types(unicode))
          | "pair_with_1" >> beam.Map(lambda x: (x, 1))
          | "group" >> beam.GroupByKey()
          | "count" >> beam.MapTuple(lambda x, ones: (x, sum(ones)))
         )

output = counts | 'format' >> beam.MapTuple(lambda word, c: '%s: %s' % (word, c))
output | 'write' >> WriteToText(output_file)
```
CPU times: user 4 µs, sys: 2 µs, total: 6 µs
Wall time: 11 µs

```python
%time
start_time = time.time()
result = p.run()
print(input_file, 'Pipeline runtime = ', time.time()-start_time)

result.wait_until_finish()
start_time = time.time()
word_lengths_filter = MetricsFilter().with_name('word_len_dist')
query_result = result.metrics().query(word_lengths_filter)
if query_result['distributions']:
    word_lengths_dist = query_result['distributions'][0]
    print( 'Average word length: ' + str(word_lengths_dist.committed.mean))
print('word length runtime = ', time.time()-start_time)

start_time = time.time()

num_words_filer = MetricsFilter().with_name('num_words')
query_result = result.metrics().query(num_words_filer)
if query_result['counters']:
    total_words = query_result['counters'][0]
    print ('Number of total words: ' + str(total_words.committed))
print('total words runtime = ', time.time()-start_time)
```
CPU times: user 5 µs, sys: 1e+03 ns, total: 6 µs
Wall time: 12.2 µs
```python
%time
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DirectRunner'
p = beam.Pipeline(options=options)

lines = p | 'read' >> ReadFromText(input_file2)

counts = (lines
          | "split" >> (beam.ParDo(WordExtractingDoFn()))
                        #.with_output_types(unicode))
          | "pair_with_1" >> beam.Map(lambda x: (x, 1))
          | "group" >> beam.GroupByKey()
          | "count" >> beam.MapTuple(lambda x, ones: (x, sum(ones)))
         )


output = counts | 'format' >> beam.MapTuple(lambda word, c: '%s: %s' % (word, c))
output | 'write' >> WriteToText(output_file)
```
CPU times: user 5 µs, sys: 1 µs, total: 6 µs
Wall time: 12.2 µs
```python
%time
start_time = time.time()
result = p.run()
print(input_file2, 'Pipeline runtime = ', time.time()-start_time)

result.wait_until_finish()
start_time = time.time()
word_lengths_filter = MetricsFilter().with_name('word_len_dist')
query_result = result.metrics().query(word_lengths_filter)
if query_result['distributions']:
    word_lengths_dist = query_result['distributions'][0]
    print( 'Average word length: ' + str(word_lengths_dist.committed.mean))
print('word length runtime = ', time.time()-start_time)

start_time = time.time()

num_words_filer = MetricsFilter().with_name('num_words')
query_result = result.metrics().query(num_words_filer)
if query_result['counters']:
    total_words = query_result['counters'][0]
    print ('Number of total words: ' + str(total_words.committed))
print('total words runtime = ', time.time()-start_time)
```
CPU times: user 5 µs, sys: 1e+03 ns, total: 6 µs
Wall time: 11 µs