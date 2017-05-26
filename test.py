from surprise import KNNBasic
from surprise import Dataset
from surprise import Reader
from surprise import evaluate, print_perf
import os
 
# path to dataset file
file_path = os.path.expanduser('C:\Users\yukikari\Documents\MyEclipse2015\LastFM\\rawdata.txt')
 
# As we're loading a custom dataset, we need to define a reader. In the
# movielens-100k dataset, each line has the following format:
# 'user item rating timestamp', separated by '\t' characters.
reader = Reader(line_format='user item rating', sep='\t')
 
data = Dataset.load_from_file(file_path, reader=reader)
data.split(n_folds=3)
 
bsl_options = {'method': 'als',
               'n_epochs': 20,
               }
sim_options = {'name': 'pearson_baseline'}
algo = KNNBasic(bsl_options=bsl_options, sim_options=sim_options)
 
# Evaluate performances of our algorithm on the dataset.
perf = evaluate(algo, data, measures=['RMSE', 'MAE'])
 
print_perf(perf)