import base64
import json
import logging
import os
import sys
import time
import ctypes
import array
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import ROOT

import boto3
import botocore
import cloudpickle as pickle


class AWSServiceWrapper:
    INVOCATION_RETRIALS_COUNT = 1  # 3

    def __init__(self, region):
        self.region = region

    def invoke_worker_lambda(self,
                           root_range,
                           script,
                           certs,
                           headers,
                           prefix,
                           logger=logging.getLogger()
                           ) -> Optional[str]:
        """
        Invoke root lambda.
        Args:
            root_range (Range): Range of data.
            script (function): A function that performs an operation on
                a range of data.
            logger (logging.Logger):
        Returns:
            Optional[str]: Name of file created by lambda or None if procedure
                has failed.
        """

        config = botocore.config.Config(retries={'total_max_attempts': 1},
                                        read_timeout=900,
                                        connect_timeout=900
                                        )
        client = boto3.client('lambda', region_name=self.region, config=config)

        payload = json.dumps({
            'range': self.encode_object(root_range),
            'script': script,
            'cert': base64.b64encode(certs).decode(),
            'headers': headers,
            'prefix': prefix,
            'S3_ACCESS_KEY': self.encode_object(os.getenv('S3_ACCESS_KEY')),
            'S3_SECRET_KEY': self.encode_object(os.getenv('S3_SECRET_KEY')),
        })

        client.invoke(
            FunctionName='worker_root_lambda',
            InvocationType='Event',
            Payload=bytes(payload, encoding='utf8')
        )

    def invoke_replicate_lambda(self,
                           ranges,
                           certs,
                           logger=logging.getLogger()
                           ) -> Optional[str]:

        config = botocore.config.Config(retries={'total_max_attempts': 1},
                                        read_timeout=900,
                                        connect_timeout=900
                                        )
        client = boto3.client('lambda', region_name=self.region, config=config)

        payload = json.dumps({
            'ranges': self.encode_object(ranges),
            'cert': base64.b64encode(certs).decode(),
        })

        try:
            response = client.invoke(
                FunctionName='replicator_root_lambda',
                InvocationType='RequestResponse',
                Payload=bytes(payload, encoding='utf8')
            )
            payload = self.get_response_payload(response)

            if 'FunctionError' in response:
                raise Exception(payload)
        except Exception as e:
            logger.error(e)

    def invoke_reduce_lambda(self,
                           reducer,
                           file_count,
                           prefix,
                           logger=logging.getLogger()
                           ) -> Optional[str]:

        config = botocore.config.Config(retries={'total_max_attempts': 1},
                                        read_timeout=900,
                                        connect_timeout=900
                                        )
        client = boto3.client('lambda', region_name=self.region, config=config)

        payload = json.dumps({
            'reducer': reducer,
            'filesno': file_count,
            'prefix': prefix,
        })

        try:
            response = client.invoke(
                FunctionName='reducer_root_lambda',
                InvocationType='RequestResponse',
                Payload=bytes(payload, encoding='utf8')
            )
            payload = self.get_response_payload(response)

            if 'FunctionError' in response or payload.get('statusCode') == 500:
                exception, msg = self.process_lambda_error(payload)
                raise exception(msg)

            filename = json.loads(payload.get('filename', 'null'))

        except Exception as e:
            logger.error(e)

        return filename

    @staticmethod
    def get_response_payload(response):
        try:
            return json.loads(response['Payload'].read())
        except Exception:
            return {}

    @staticmethod
    def process_lambda_error(payload):
        try:
            # Get error specification and remove additional
            # quotas (side effect of serialization)
            error_type = payload['errorType'][1:-1]
            error_message = payload['errorMessage'][1:-1]
            exception = getattr(sys.modules['builtins'], error_type)
            msg = f"Lambda raised an exception: {error_message}"
        except Exception:
            exception = RuntimeError
            msg = (f"Lambda raised an exception: (type={payload['errorType']},"
                   f"message={payload['errorMessage']})")
        return exception, msg

    def get_and_deserialize_object_from_s3(self, filename, bucket_name):
        pickled_file = self.get_file_content_from_s3(filename, bucket_name)
        return pickle.loads(pickled_file)

    def get_file_content_from_s3(self, filename, bucket_name):
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=filename)
        return response['Body'].read()

    def clean_s3_bucket(self, bucket_name):
        s3_resource = boto3.resource('s3', region_name=self.region)
        s3_bucket = s3_resource.Bucket(name=bucket_name)
        s3_bucket.objects.all().delete()

    def clean_s3_prefix(self, bucket_name, prefix):
        s3_resource = boto3.resource('s3', region_name=self.region)
        s3_bucket = s3_resource.Bucket(name=bucket_name)
        s3_bucket.objects.all().filter(Prefix=prefix).delete()

    def s3_is_empty(self, bucket_name, prefix):
        s3_resource = boto3.resource('s3')
        s3_bucket = s3_resource.Bucket(name=bucket_name)
        return len(list(s3_bucket.objects.all().filter(Prefix=prefix))) == 0

    def s3_object_exists(self, bucket_name, filename):
        s3_resource = boto3.resource('s3', region_name=self.region)
        try:
            s3_resource.Object(bucket_name, filename).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != "404":
                print(e)
            return False
        else:
            return True

    def get_ssm_parameter_value(self, name):
        ssm_client = boto3.client('ssm', region_name=self.region)
        param = ssm_client.get_parameter(Name=name)
        return param['Parameter']['Value']

    @staticmethod
    def encode_object(object_to_encode) -> str:
        return base64.b64encode(pickle.dumps(object_to_encode)).decode()

    def get_from_s3(self, filename, bucket_name, directory):
        local_filename = os.path.join(directory, filename)
        s3_client = boto3.client('s3', region_name=self.region)
        s3_client.download_file(bucket_name, filename, local_filename)

        # tfile = ROOT.TFile(local_filename, 'OPEN')
        # result = []

        # Get all objects from TFile
        # for key in tfile.GetListOfKeys():
        #    result.append(key.ReadObj())
        #    result[-1].SetDirectory(0)
        # tfile.Close()
        with open(local_filename, 'rb') as pickle_file:
            result = pickle.load(pickle_file)

        # Remove temporary root file
        Path(local_filename).unlink()

        return result

    @staticmethod
    def list_all_parts(Bucket: str, Key: str, UploadId: str, **kwargs):
        s3 = boto3.client('s3')

        kwargs['Bucket'] = Bucket
        kwargs['Key'] = Key
        kwargs['UploadId'] = UploadId
        kwargs['MaxParts'] = 1000

        parts = []

        next_part = None
        while True:
            if next_part:
                kwargs['PartNumberMarker'] = next_part
            response = s3.list_parts(**kwargs)
            parts.extend(response.get('Parts', []))
            if not response.get('IsTruncated'):
                break
            next_part = response.get('NextPartNumberMarker')

        return parts

    def stream_cp(self, filename: str, new_filename: str, bucket: str, buff_size: int):
        s3 = boto3.client('s3')

        response = s3.create_multipart_upload(Bucket=bucket, Key=new_filename)
        id = response['UploadId']
        parts = []

        for i, part in enumerate(stream_read_rootfile(filename, buff_size)):
            response = s3.upload_part(Body=part, Bucket=bucket, Key=new_filename, PartNumber=i+1, UploadId=id)
            parts.append({
                'ETag': response['ETag'],
                'PartNumber': i + 1,
            })

        response = s3.complete_multipart_upload(Bucket=bucket, Key=new_filename, MultipartUpload=dict(Parts=parts), UploadId=id)

    def start_multipart_upload(self, bucket: str, filename: str):
        s3 = boto3.client('s3')
        response = s3.create_multipart_upload(Bucket=bucket, Key=filename)
        return response['UploadId']

    def finish_multipart_upload(self, bucket, filename, parts, uid):
        s3 = boto3.client('s3')
        s3.complete_multipart_upload(Bucket=bucket, Key=filename, MultipartUpload=dict(Parts=parts), UploadId=uid)

def stream_read_rootfile(filename: str, buff_size: int):
    f = ROOT.TFile.Open(filename)
    # Maximum number of parts per upload is 10000
    buff_size = max(buff_size, f.GetSize() // 9999)
    buff = array.array('b', b'\x00' * buff_size)
    buffptr = ctypes.c_char_p(buff.buffer_info()[0])

    for pos in range(0, f.GetSize(), buff_size):
        f.ReadBuffer(buffptr, pos, buff_size)
        yield buff.tobytes()

    f.Close()

def root_file_size(filename: str):
    f = ROOT.TFile.Open(filename)
    size = f.GetSize()
    f.Close()
    return size
