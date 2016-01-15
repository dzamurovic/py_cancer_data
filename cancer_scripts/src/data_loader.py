import os
import cancer_data_file_mapper
from persistence import CassandraRepo
from cancer_data_model import CancerData

class DataLoader:

    def __init__(self):
        self.data_dir = '../cancer_data'

    def process_directory(self):
        data_files = os.listdir(self.data_dir)
        cassandra_repo = CassandraRepo()

        for f in data_files:
            cancer_data = self._process_data_file(os.path.join(os.path.abspath(self.data_dir), f))
            cassandra_repo.store(cancer_data)


    def _process_data_file(self, file):
        data_mapper = cancer_data_file_mapper.CancerDataFileMapper()
        data_group_info = data_mapper.read_data_group_information(file)
        record_data = data_mapper.read_cancer_data(file)

        cancer_data = CancerData(
            data_group_info = data_group_info,
            cancer_data = record_data
        )
        return cancer_data

    def _persist_data(self, data_group_info, record_data):
        return None


def main():
    data_loader = DataLoader()
    data_loader.process_directory()

main()
