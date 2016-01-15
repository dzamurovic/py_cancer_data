class DataGroupInformation:

    country = ""
    age_gender = None

    def __init__(self, country, age_gender):
        self.country = country
        self.age_gender = age_gender


class AgeGender:

    age = 0
    gender = ""

    def __init__(self, age, gender):
        self.age = age
        self.gender = gender


class CancerDataRecord:

    cancer = ""
    deaths = 0
    crude_rate = 0.0
    asr = 0.0 # age standardized rate
    cumulative_risk = 0.0

    def __init__(self, cancer, deaths, crude_rate, asr, cumulative_risk):
        self.cancer = cancer
        self.deaths = deaths
        self.crude_rate = crude_rate
        self.asr = asr
        self.cumulative_risk = cumulative_risk


class CancerData:

    data_group_info = None
    cancer_data = []

    def __init__(self, data_group_info, cancer_data):
        self.data_group_info = data_group_info
        self.cancer_data = cancer_data