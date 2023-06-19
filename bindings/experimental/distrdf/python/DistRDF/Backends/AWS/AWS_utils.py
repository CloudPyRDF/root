import base64
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import boto3
import botocore
import cloudpickle as pickle


class AWSServiceWrapper:
    INVOCATION_RETRIALS_COUNT = 1  # 3

    def __init__(self, region):
        self.region = region

    def invoke_root_lambda(self,
                           root_range,
                           script,
                           certs,
                           headers,
                           bucket_name,
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

        trials = self.INVOCATION_RETRIALS_COUNT
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
            'S3_ACCESS_KEY': self.encode_object(os.getenv('S3_ACCESS_KEY')),
            'S3_SECRET_KEY': self.encode_object(os.getenv('S3_SECRET_KEY')),
        })

        filename: Optional[str] = None

        while trials > 0:
            trials -= 1
            try:
                response = client.invoke(
                    FunctionName='root_lambda',
                    InvocationType='RequestResponse',
                    Payload=bytes(payload, encoding='utf8')
                )
                payload = self.get_response_payload(response)

                if 'FunctionError' in response or payload.get('statusCode') == 500:
                    exception, msg = self.process_lambda_error(payload)
                    raise exception(msg)

                filename = json.loads(payload.get('filename', 'null'))
                monitoring_result = payload.get('body', 'null')

                path = os.getcwd()
                result_dir = path + "/results"
                if not os.path.exists(result_dir):
                    os.makedirs(result_dir)
                f = open(f'{result_dir}/{filename}.json', "a")
                f.write(monitoring_result)
                f.close()
            except Exception as e:
                logger.error(e)
            finally:
                break

            time.sleep(1)

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

    def get_partial_result_from_s3(self, filename, bucket_name):
        pickled_file = self.get_file_content_from_s3(filename, bucket_name)
        return pickle.loads(pickled_file)

    def get_file_content_from_s3(self, filename, bucket_name):
        s3_client = boto3.client('s3', region_name=self.region)
        response = s3_client.get_object(Bucket=bucket_name, Key=filename)
        return response['Body'].read()

    def clean_s3_bucket(self, bucket_name):
        s3_resource = boto3.resource('s3', region_name=self.region)
        s3_bucket = s3_resource.Bucket(name=bucket_name)
        s3_bucket.objects.all().delete()

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
