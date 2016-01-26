import numpy as np
import pandas as pd
import persistence

class DataProcessor:

    def do_the_thing(self, toProcess):
        variables = toProcess[0].keys()
        df = pd.DataFrame([[getattr(i,j) for j in variables] for i in toProcess], columns = variables)

        df_deaths = df[['age', 'gender', 'cancer', 'cumulative_risk', 'deaths']]
        deaths_grouped = df_deaths.groupby(['age', 'gender'], sort=True)
        deaths_summed = deaths_grouped.sum()
        #print(deaths_summed)

        male_occurrences = df_deaths[df_deaths['gender'] == 'Male']
        #print(male_occurrences)

        # how to split in two data frames by gender by creating a dictionary that will contain those dataframes
        genders = df.gender.unique()
        data_frame_dict = {elem : df for elem in genders}
        for key in data_frame_dict.keys():
            data_frame_dict[key] = df[:][df.gender == key]

        print(data_frame_dict)

