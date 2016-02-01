from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.models import columns
from cassandra.cqlengine.query import BatchQuery

class CancerDataEntity(Model):

    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64',
                                  'tombstone_threshold': '.2'}
                   }
    __table_name__ = "cancer_data"

    age = columns.Text(primary_key=True)
    gender = columns.Text(primary_key=True)
    country = columns.Text(primary_key=True)
    cancer = columns.Text(primary_key=True)
    asr = columns.Float()
    crude_rate = columns.Float()
    cumulative_risk = columns.Float()
    deaths = columns.Integer()

class CancerDeathsEntity(Model):

    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64',
                                  'tombstone_threshold': '.2'}
                   }
    __table_name__ = "cancer_deaths_data"

    cancer = columns.Text(primary_key=True)
    deaths = columns.Integer()

class CancerGenderDeathsEntity(Model):

    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64',
                                  'tombstone_threshold': '.2'}
                   }
    __table_name__ = "cancer_gender_deaths_data"

    gender = columns.Text(primary_key=True)
    cancer = columns.Text(primary_key=True)
    deaths = columns.Integer()

class CancerAgeDeathsEntity(Model):

    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64',
                                  'tombstone_threshold': '.2'}
                   }
    __table_name__ = "cancer_age_deaths_data"

    age = columns.Text(primary_key=True)
    cancer = columns.Text(primary_key=True)
    deaths = columns.Integer()

class CancerAgeGenderRiskEntity(Model):

    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64',
                                  'tombstone_threshold': '.2'}
                   }
    __table_name__ = "cancer_age_gender_risk_data"

    age = columns.Text(primary_key=True)
    gender = columns.Text(primary_key=True)
    cancer = columns.Text(primary_key=True)
    cumulative_risk = columns.Float()

class CassandraRepo:

    def __init__(self):
        self.key_space = 'cancer_app'
        connection.setup(['127.0.0.1'], self.key_space, protocol_version=3)
        sync_table(CancerDataEntity)
        sync_table(CancerGenderDeathsEntity)
        sync_table(CancerDeathsEntity)
        sync_table(CancerAgeDeathsEntity)
        sync_table(CancerAgeGenderRiskEntity)


    def store_cancer_data(self, cancer_data):
        data_group_info = cancer_data.data_group_info

        country = data_group_info.country
        age = data_group_info.age_gender.age
        gender = data_group_info.age_gender.gender

        bq = BatchQuery(consistency=ConsistencyLevel.ONE)
        for cancer_data_record in cancer_data.cancer_data:
            cancer = cancer_data_record.cancer
            deaths = cancer_data_record.deaths
            crude_rate = cancer_data_record.crude_rate
            asr = cancer_data_record.crude_rate
            cumulative_risk = cancer_data_record.cumulative_risk
            CancerDataEntity.batch(bq).create(age = age, gender = gender, country = country, cancer = cancer, asr = asr, crude_rate = crude_rate, cumulative_risk = cumulative_risk, deaths = deaths)
        bq.execute()

    def _store_cancer_deaths_data(self, row):
        entity = CancerDeathsEntity()
        entity.cancer = row['cancer']
        entity.deaths = row['deaths']
        entity.save()


    def store_cancer_deaths_data(self, cancer_deaths_df):
        cancer_deaths_df.reset_index().apply(self._store_cancer_deaths_data, axis='columns')

    def _store_cancer_gender_deaths_data(self, row):
        entity = CancerGenderDeathsEntity()
        entity.cancer = row['cancer']
        entity.gender = row['gender']
        entity.deaths = row['deaths']
        entity.save()

    def store_cancer_gender_deaths_data(self, cancer_gender_df):
        cancer_gender_df.reset_index().apply(self._store_cancer_gender_deaths_data, axis='columns')

    def _store_cancer_age_deaths_data(self, row):
        entity = CancerAgeDeathsEntity()
        entity.cancer = row['cancer']
        entity.age = row['age']
        entity.deaths = row['deaths']
        entity.save()

    def store_cancer_age_deaths_data(self, cancer_age_df):
        cancer_age_df.reset_index().apply(self._store_cancer_age_deaths_data, axis='columns')

    def _store_cancer_age_gender_risk_data(self, row):
        entity = CancerAgeGenderRiskEntity()
        entity.cancer = row['cancer']
        entity.age = row['age']
        entity.gender = row['gender']
        entity.cumulative_risk = row['cumulative_risk']
        entity.save()

    def store_cancer_age_gender_risk_data(self, cancer_age_gender_risk_df):
        cancer_age_gender_risk_df.reset_index().apply(self._store_cancer_age_gender_risk_data, axis='columns')

    def load_all(self):
        data = []
        for cancer_data in CancerDataEntity.all():
            data.append(cancer_data)
        return data



