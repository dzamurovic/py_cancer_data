import numpy as np
import pandas as pd

class DataProcessor:

    def _group_and_sort(self, df, groupBy, sortBy):
        df_clean = df[~df.cancer.str.startswith("All")]
        df_sum = df_clean.groupby(groupBy, sort=False).sum()
        df_sorted = df_sum.sort_values(by=sortBy, ascending=False)
        return df_sorted

    """
    def _age_gender_mortality_comparison(self, df, groupBy, sortBy):
        # comparison of genders, compare number of deaths by age and gender
        df_sum = df.groupby(groupBy, sort=False).sum()
        df_sorted = df_sum.sort_values(by=sortBy, ascending=False)
        df_sorted = df_sorted.reset_index()
        genders = df_sorted.gender.unique()
        df_dict = {elem : df_sorted for elem in genders}
        for key in df_dict.keys():
            df_dict[key] = df_sorted[:][df_sorted.gender == key]

        return df_dict
    """

    def do_the_thing(self, toProcess):
        variables = toProcess[0].keys()
        df = pd.DataFrame([[getattr(i,j) for j in variables] for i in toProcess], columns = variables)

        df_deaths = self._group_and_sort(df[['cancer', 'deaths']], ['cancer'], 'deaths')
        df_deaths.to_csv('deaths.tsv', sep='\t')

        df_deaths_by_gender = self._group_and_sort(df[['gender', 'cancer', 'deaths']], ['gender'], 'deaths')
        df_deaths_by_gender.to_csv('deaths_by_gender.tsv', sep='\t')

        df_deaths_by_age = self._group_and_sort(df[['age', 'cancer', 'deaths']], ['age'], 'deaths')
        df_deaths_by_age.to_csv('deaths_by_age.tsv', sep='\t')

        df_cumulative_risk = self._group_and_sort(df[['age', 'gender', 'cancer', 'cumulative_risk']], ['age', 'gender', 'cancer'], 'cumulative_risk')
        df_cumulative_risk.to_csv('cumulative_risk.tsv', sep='\t')
