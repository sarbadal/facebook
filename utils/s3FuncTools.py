import os
import boto3
import botocore
from boto.s3.connection import S3Connection
from botocore.client import Config

from settings.settings import BASE_DIR


class FileTransferS3(object):
    """
    Combination of file upload, 
    file download and file delete methods.
    """
    name = 's3FileTransfer class'

    def __init__(self, dir_path=None, upload_files=None, download_files=None):
        (
            self.__s3_access_key,
            self.__s3_secret_key,
            self.__bucket_name,
            self.__base_path,
            self.__s3_path
        ) = self.load_credentials()

        self.dir_path = dir_path
        self.upload_files = upload_files
        self.download_files = download_files


    @property
    def s3_access_key(self):
        """Return s3_access_key"""
        return self.__s3_access_key

    @s3_access_key.setter
    def s3_access_key(self, s3_access_key):
        """Set s3_access_key"""
        self.__s3_access_key = s3_access_key

    @property
    def s3_secret_key(self):
        """Return s3_secret_key"""
        return self.__s3_secret_key

    @s3_secret_key.setter
    def s3_secret_key(self, s3_secret_key):
        """Set s3_secret_key"""
        self.__s3_secret_key = s3_secret_key

    @property
    def bucket_name(self):
        """Return bucket_name"""
        return self.__bucket_name

    @bucket_name.setter
    def bucket_name(self, bucket_name):
        """Set bucket_name"""
        self.__bucket_name = bucket_name

    @property
    def base_path(self):
        """Return base_path"""
        return self.__base_path

    @base_path.setter
    def base_path(self, base_path):
        """Set base_path"""
        self.__base_path = base_path

    @property
    def s3_path(self):
        """Return s3_path"""
        return self.__s3_path

    @s3_path.setter
    def s3_path(self, s3_path):
        """Set s3_path"""
        self.__s3_path = s3_path


    @staticmethod
    def load_credentials(credentils_json=None, file=False):
        """
        Loding credentials from json file
        Default locatiom: credentials\credentials.json file
        """
        import json

        if credentils_json is None:
            filename = os.path.join(BASE_DIR, 'credentials', 'credentials.json')

            with open(filename, 'r') as f:
                c = json.load(f)['S3']
                return c['s3_access_key'], c['s3_secret_key'], c['bucket_name'], c['base_path'], c['s3_path']

        elif file:
            with open(file, 'r') as f:
                c = json.load(f)['S3']
                return c['s3_access_key'], c['s3_secret_key'], c['bucket_name'], c['base_path'], c['s3_path']

        else:
            try:
                c = credentils_json
                return c['s3_access_key'], c['s3_secret_key'], c['bucket_name'], c['base_path'], c['s3_path']

            except:
                try:
                    c = credentils_json['S3']
                    return c['s3_access_key'], c['s3_secret_key'], c['bucket_name'], c['base_path'], c['s3_path']
                except:
                    print("Couldn't load credentials")
                    return None, None, None, None, None


    def _s3_connection(self):
        """Connection"""
        try:
            return boto3.resource(
                's3',
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                config=Config(signature_version='s3v4'),
                verify=False
            )
        except:
            print('Could not establish connection.')
            return None


    @property
    def name(self):
        """Getter method - name"""
        return self._name


    def uploadfile_to_s3(self, from_dir=None, s3_dir=None, files=None):
        """Upload file to a s3 Bucket."""
        _s3 = self._s3_connection()

        if files is None:
            files = self.upload_files

        if from_dir is None:
            from_dir = self.dir_path

        if s3_dir is None:
            s3_dir = self.s3_path

        for f in files:
            file_dir = os.path.join(from_dir, f)
            s3_file = os.path.join(s3_dir, f).replace('\\', '/')
            print(s3_file)

            with open(file_dir, 'rb') as df:
                print(f'Uploading {df.name} ==> {s3_file}')
                _s3.Bucket(self.bucket_name).upload_file(df.name, s3_file)
        print(f'All {len(files)} file(s) have been uploaded successfully to s3 bucket')


    def downloadfile_from_s3(self, out_file_path=None, files=None):
        """Download files from S3."""
        _s3 = self._s3_connection()

        if files is None:
            files = self.download_files

        if not os.path.exists(out_file_path):
            os.makedirs(out_file_path)

        for f in files:
            f = os.path.join(self.s3_path, f).replace('\\', '/')
            try:
                _s3.Bucket(self.bucket_name).download_file(
                    f, 
                    os.path.join(out_file_path, os.path.basename(f))
                )
                print(f'{os.path.basename(f)} has been downloeded.')

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print('The object does not exist.')
                    return None
                else:
                    raise


    @property
    def all_buckets(self):
        """Get the list of all Buckets."""
        _s3 = self._s3_connection()

        bucket_list = []
        for _bucket in _s3.buckets.all():
            print(bucket_list.append(_bucket.name))

        return bucket_list


    @property
    def files_root(self):
        """Get the list of files."""
        _s3 = self._s3_connection()

        files = []
        bucket = _s3.Bucket(self.bucket_name)

        for f in bucket.objects.all():
            files.append(f.key)

        return files

        
    def __get_bucket(self):
        """Get the bucket."""
        conn = S3Connection(self.s3_access_key, self.s3_secret_key)
        return conn.get_bucket(self.bucket_name, validate=False)


    def get_files_from_s3(self, bucket=None, prefix=None, delimiter=None, verbose=False):
        """
        Get the list.
        Params:
            prefix - Example 'abc/'. It will list down all files after abc/.
        """
        if bucket is None:
            mybucket = self.__get_bucket()

        else:
            conn = S3Connection(self.s3_access_key, self.s3_secret_key)
            mybucket = conn.get_bucket(self.bucket_name,  validate=False)

        if prefix is None:
            prefix = self.base_path

        files = []
        f_list = mybucket.list(prefix=prefix, delimiter=delimiter)
        for f in f_list:
            if str(f.name) != prefix:
                if verbose:
                    print(str(f.name).replace(prefix, ''))

                files.append(str(f.name).replace(prefix, ''))

        return files


    def deletefile_from_s3(self, prefix=None):
        """Delete file/folder from S3 bucket."""
        # Prefix='incoming/file_2_delete'
        _s3 = self._s3_connection()
        bucket = _s3.Bucket(self.bucket_name)

        if prefix:
            for obj in bucket.objects.filter(Prefix=prefix):
                _s3.Object(bucket.name, obj.key).delete()
            print('All files have been deleted from {}'.format(prefix))
        else:
            print('Prefix cannt be None')
            return None


    def __repr__(self):
        """
        Need this so that a user friendly results comes if print
        """
        _dir_path = self.dir_path
        if _dir_path is None:
            _dir_path = 'None'
        else:
            _dir_path = "'" + str(_dir_path) + "'"

        _upload_files = self.upload_files
        if _upload_files is None:
            _upload_files = 'None'
        else:
            _upload_files = "'" + str(_upload_files) + "'"

        _download_files = self.download_files
        if _download_files is None:
            _download_files = 'None'
        else:
            _download_files = "'" + str(_download_files) + "'"

        txt = f"FileTransferS3(s3_access_key='{self.s3_access_key}', s3_secret_key='{self.s3_secret_key}',"
        txt += f" bucket_name='{self.bucket_name}', base_path='{self.base_path}', dir_path={_dir_path},"
        txt += f" s3_path='{self.s3_path}', upload_files={_upload_files}, download_files={_download_files})"

        return txt


if __name__ == '__main__':
    from Settings.settings import BASE_DIR
 
    ft = FileTransferS3()
    # print(ft)
    # for f in ft.files_root:
    #     print(f)

    files = ft.get_files_from_s3()
    for f in files:
        print(f)


