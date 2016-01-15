import string
import cancer_data_model
from decimal import Decimal, InvalidOperation

class CancerDataFileMapper:

    def read_data_group_information(self, file):
        print('\n' + file)

        line_counter = 1
        with open(file) as f:
            for line in f:
                line = line.rstrip()
                if line_counter == 1:
                    country = self._parse_country(line)
                elif line_counter == 2:
                    age_gender = self._parse_age_and_gender(line)
                else:
                    break

                line_counter += 1

            data_group_info = cancer_data_model.DataGroupInformation(country = country, age_gender = age_gender)
        return data_group_info

    def read_cancer_data(self, file):
        data = []

        line_counter = 1
        with open(file) as f:
            for line in f:
                line = line.rstrip()
                if line_counter > 3:
                    cancer_data = self._parse_data_record(line)
                    data.append(cancer_data)

                line_counter += 1

        return data

    def _parse_country(self, line):
        end_idx = string.find(line, '(') - 1
        return line[:end_idx]

    def _parse_gender(self, line):
        end_idx = string.find(line, ',')
        return line[:end_idx]

    def _parse_age(self, line):
        marker = 'age '
        start_idx = string.find(line, marker) + len(marker)
        return line[start_idx:]

    def _parse_age_and_gender(self, line):
        age_gender = cancer_data_model.AgeGender(age = self._parse_age(line), gender = self._parse_gender(line))
        return age_gender

    def _parse_data_record(self, line):
        tokens = line.split('\t')
        val_cancer = tokens[0]
        try:
            val_deaths = int(tokens[1])
        except ValueError:
            val_deaths = -1

        try:
            val_crude_rate = Decimal(tokens[2])
        except InvalidOperation:
            val_crude_rate = Decimal('-1.0')

        try:
            val_asr = Decimal(tokens[3])
        except InvalidOperation:
            val_asr = Decimal('-1.0')

        try:
            val_cumulative_risk = Decimal(tokens[4])
        except InvalidOperation:
            val_cumulative_risk = Decimal('-1.0')

        cancer_data_record = cancer_data_model.CancerDataRecord(cancer = val_cancer, deaths = val_deaths, crude_rate = val_crude_rate, asr = val_asr, cumulative_risk = val_cumulative_risk)
        return cancer_data_record

