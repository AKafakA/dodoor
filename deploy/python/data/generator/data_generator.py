from abc import ABC, abstractmethod

from schema import Schema

MIN_CORES = 1


class DataGenerator(ABC):
    input_data_schema = Schema({
        "taskId": int,
        "cores": float,
        "memory": int,
        "disk": int,
        "duration": int,
        "startTime": int,
    })

    @abstractmethod
    def generate(self, num_records, start_id, max_duration=-1, time_range_in_days=None):
        """
        Generate data for the given number of records
        :param num_records: the number of records to generate
        :param start_id: the first id to start with
        :param max_duration: used for filter out records with duration greater than this value, -1 means no filter
        :param time_range_in_days: the time range in days to filter the records submitted not in this range
        :return:
        """
        raise NotImplementedError()

    def validate_data(self, data):
        self.input_data_schema.validate(data)
        return True

    def write_data_target_output(self, data, output_path):
        print("Data length: {}".format(len(data)))
        with open(output_path, "w+") as f:
            for record in data:
                self.validate_data(record)
                output = ",".join([str(value) for value in record.values()])
                f.write(output + "\n")
        return True
