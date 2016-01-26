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


class CassandraRepo:

    def __init__(self):
        #self.cluster = Cluster(
        #    contact_points=['127.0.0.1'],
        #    load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
        #    default_retry_policy=RetryPolicy()
        #)
        self.key_space = 'cancer_app'
        connection.setup(['127.0.0.1'], self.key_space, protocol_version=3)
        sync_table(CancerDataEntity)

    def store(self, cancer_data):
        data_group_info = cancer_data.data_group_info

        country = data_group_info.country
        age = data_group_info.age_gender.age
        gender = data_group_info.age_gender.gender

        #session = self.cluster.connect(self.key_space)

        #insert_statement = session.prepare('INSERT INTO cancer_data (age, gender, country, cancer, asr, crude_rate, cumulative_risk, deaths) values (?, ?, ?, ?, ?, ?, ?, ?)')
        #batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

        bq = BatchQuery(consistency=ConsistencyLevel.ONE)
        for cancer_data_record in cancer_data.cancer_data:
            cancer = cancer_data_record.cancer
            deaths = cancer_data_record.deaths
            crude_rate = cancer_data_record.crude_rate
            asr = cancer_data_record.crude_rate
            cumulative_risk = cancer_data_record.cumulative_risk
            CancerDataEntity.batch(bq).create(age = age, gender = gender, country = country, cancer = cancer, asr = asr, crude_rate = crude_rate, cumulative_risk = cumulative_risk, deaths = deaths)
            #batch.add(insert_statement, (age, gender, country, cancer, asr, crude_rate, cumulative_risk, deaths))
        bq.execute()

        #session.execute(batch)


    def load_all(self):
        data = []
        for cancer_data in CancerDataEntity.all():
            data.append(cancer_data)
        return data



