import numpy as np
import pandas as pd
import persistence

class DataProcessor:

    def do_the_thing(self, toProcess):
        variables = toProcess[0].keys()
        df = pd.DataFrame([[getattr(i,j) for j in variables] for i in toProcess], columns = variables)
        #df.to_csv(path_or_buf="data_grouped.txt", sep="\t", index=False)