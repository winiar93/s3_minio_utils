import urllib.request
import minio
from minio import Minio
from minio.deleteobjects import DeleteObject


class MinioS3:
    conn_dict = {"endpoint": "YOUR_S3_SERVER", "access_key": "##############",
                 "secret_key": "##############", "region": "YOUR_REGION"}

    s3_name = "S3_BUCKET_NAME"
    conn = Minio(**conn_dict)

    def minio_list(self):
        o = [x.object_name for x in self.conn.list_objects(self.s3_name, recursive=True)]
        for x in o:
            result = self.conn.stat_object(self.s3_name, x)
            print(x, result.last_modified, str(round(result.size/(1024*1024), 4)) + ' MB')

    def delete_by_iteration(self):
        try:
            objects = [o.object_name for o in self.conn.list_objects(self.s3_name, recursive=True)]
            for o in objects:
                self.conn.remove_object('BUCKET_NAME', o)
                print("REMOVED : " + o)

        except Exception as e:
            print("Error occured : " + str(e))

    def save_file_s3(self, file_id, output_name_extension):
        try:
            file = self.conn.get_object('BUCKET_NAME', file_id)
            read_file = file.read()
            f = open(f"{output_name_extension}", "wb")
            f.write(read_file)
            f.close()
        except Exception as e:
            print("Problem occured while saving file: " + str(e))


def choose_option(operation):
    operation_minio = MinioS3()
    if operation == "del":
        operation_minio.delete_by_iteration()
    elif operation == "list":
        print(operation_minio.minio_list())


choose_option("list")