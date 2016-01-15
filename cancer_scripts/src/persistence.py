from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

class CassandraRepo:

    def __init__(self):
        self.cluster = Cluster(
            contact_points=['127.0.0.1'],
            load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
            default_retry_policy=RetryPolicy()
        )
        self.key_space = 'cancer_app'

    def store(self, cancer_data):
        data_group_info = cancer_data.data_group_info

        country = data_group_info.country
        age = data_group_info.age_gender.age
        gender = data_group_info.age_gender.gender

        session = self.cluster.connect(self.key_space)

        insert_statement = session.prepare('INSERT INTO cancer_data (age, gender, country, cancer, asr, crude_rate, cumulative_risk, deaths) values (?, ?, ?, ?, ?, ?, ?, ?)')
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

        for cancer_data_record in cancer_data.cancer_data:
            cancer = cancer_data_record.cancer
            deaths = cancer_data_record.deaths
            crude_rate = cancer_data_record.crude_rate
            asr = cancer_data_record.crude_rate
            cumulative_risk = cancer_data_record.cumulative_risk
            #print(age + " " + gender + " " + country+ " " + cancer + " " + str(deaths) + " " + str(crude_rate) + " " + str(asr) + " " + str(cumulative_risk))
            batch.add(insert_statement, (age, gender, country, cancer, asr, crude_rate, cumulative_risk, deaths))

        session.execute(batch)
