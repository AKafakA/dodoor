import sqlite3


class TableKeys:
    VM_TYPE_NAME = "vmType"
    VM_NAME = "vm"
    VM_ID = "vmId"
    VM_TYPE_ID = "vmTypeId"
    RESOURCE_TYPE = ["core", "memory", "ssd"]
    START_TIME = "starttime"
    END_TIME = "endtime"
    MACHINE_ID = "machineId"


# The reader class to access the VM trace data in sqlite3 format
class AzureSqliteProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def read(self, query):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def get_num_vm_requests(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT({}) FROM {}".format(TableKeys.VM_ID, TableKeys.VM_NAME))
        return cursor.fetchall()

    def get_vm_type(self, type_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = ("SELECT * FROM {} WHERE {} = {}"
                 .format(TableKeys.VM_TYPE_NAME, TableKeys.VM_TYPE_ID, type_id))
        cursor.execute(query)
        return cursor.fetchall()

    def get_vm_request(self, vm_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = ("SELECT * FROM {} WHERE {} = {}"
                 .format(TableKeys.VM_NAME, TableKeys.VM_ID, vm_id))
        cursor.execute(query)
        return cursor.fetchall()

    def get_vm_resource_requests_in_batch(self, machine_id, num_requests_per_machine):
        """
        Get the vm resource requests in batch for a specific machine in azure data
            (since no heterogeneous cloud have not been discussed yet)
        :param num_requests_per_machine: the number of requests to get for each machine id
        :param machine_id: the id used to select the trace of a specific machine in azure
        :return: the list of vm resource requests
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        keys = [TableKeys.VM_ID]
        resource_keys = [key for key in TableKeys.RESOURCE_TYPE]
        keys.extend(resource_keys)
        time_keys = [TableKeys.START_TIME, TableKeys.END_TIME]
        keys.extend(time_keys)
        keys_query = ",".join(keys)
        query = (("SELECT {} " +
                  "FROM {} LEFT JOIN {} On {}.{} = {}.{} WHERE {}.{} = {}")
                 .format(keys_query, TableKeys.VM_NAME,
                         TableKeys.VM_TYPE_NAME, TableKeys.VM_NAME,
                         TableKeys.VM_TYPE_ID,
                         TableKeys.VM_TYPE_NAME, TableKeys.VM_TYPE_ID,
                         TableKeys.VM_TYPE_NAME, TableKeys.MACHINE_ID, machine_id))

        cursor.execute(query)
        if num_requests_per_machine == -1:
            values = cursor.fetchall()
        else:
            values = cursor.fetchmany(num_requests_per_machine)
        resources_requests = []
        for value in values:
            resources_requests.append(
                {keys[i]: value[i] for i in range(len(keys))})
        return resources_requests
