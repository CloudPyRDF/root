import logging
import functools
import time
import uuid

from collections import namedtuple

from DistRDF import DataFrame
from DistRDF import HeadNode
from DistRDF.Backends import Base

from .flushing_logger import FlushingLogger
from .AWS_utils import AWSServiceWrapper
from .krbcert import KRBCert
from .reducer import Reducer
from .replicator import Replicator
from concurrent.futures import ThreadPoolExecutor

import ROOT

AWSBENCH = namedtuple("AWSBENCH", ["npartitions", "mapwalltime", "reducewalltime"])


class AWS(Base.BaseBackend):
    """
    Backend that executes the computational graph using using AWS Lambda
    for distributed execution.
    """

    MIN_NPARTITIONS = 8
    npartitions = 32

    def __init__(self, region_name):
        """
        Config for AWS is same as in Dist backend,
        more support will be added in future.
        """
        super(AWS, self).__init__()
        self.logger = FlushingLogger() if logging.root.level >= logging.INFO else logging.getLogger()
        self.npartitions = self._get_partitions()
        self.paths = []
        self.cert = KRBCert()
        self.aws = AWSServiceWrapper(region_name)
        self.processing_bucket = self.aws.get_ssm_parameter_value('processing_bucket')

    def _get_partitions(self):
        return int(self.npartitions or AWS.MIN_NPARTITIONS)

    def make_dataframe(self, *args, **kwargs):
        """
        Creates an instance of distributed RDataFrame that can send computations
        to a Spark cluster.
        """
        # Set the number of partitions for this dataframe, one of the following:
        # 1. User-supplied `npartitions` optional argument
        # 2. An educated guess according to the backend, using the backend's
        #    `optimize_npartitions` function
        # 3. Set `npartitions` to 2
        npartitions = kwargs.pop("npartitions", self.optimize_npartitions())
        headnode = HeadNode.get_headnode(npartitions, *args)
        if kwargs.pop('replicate', False):
            # Stop annoying messages that flood the screen when
            # printed by multiple threads
            ROOT.gErrorIgnoreLevel = ROOT.kWarning
            headnode = self.replicated_headnode(headnode)

        return DataFrame.RDataFrame(headnode, self)

    def replicated_headnode(self, headnode: HeadNode):
        if not hasattr(headnode, 'tree'):
            raise ValueError('No file to replicate')

        files = [f.GetTitle() for f in headnode.tree.GetListOfFiles()]
        replicator = Replicator(self.processing_bucket, self.aws.region, lambdas_count=16)
        newfiles = replicator.replicate(files)

        args = [
            headnode.npartitions,
            headnode.tree.GetName(),
            newfiles,
        ]
        if headnode.defaultbranches:
            args.append(headnode.defaultbranches)

        return HeadNode.get_headnode(*args)

    def ProcessAndMerge(self, ranges, mapper, reducer):
        """
        Performs map-reduce using AWS Lambda.
        Args:
            mapper (function): A function that runs the computational graph
                and returns a list of values.
            reducer (function): A function that merges two lists that were
                returned by the mapper.
        Returns:
            list: A list representing the values of action nodes returned
            after computation (Map-Reduce).
        """

        prefix = str(uuid.uuid1())
        lambdas_count = len(ranges)

        invoke_worker = self.create_init_arguments(
            mapper,
            lambdas_count,
            prefix
        )
        self.aws.serialize_and_upload_to_s3(self.processing_bucket, reducer, f'output/{prefix}/reducer')
        self.logger.info(f'Before lambdas invoke. Number of lambdas: {lambdas_count}')

        invoke_begin = time.time()

        self.work(invoke_worker, ranges)

        self.logger.info(f'Lambdas finished.')


        reduce_begin = time.time()

        filename = f'output/{prefix}/final'
        self.aws.s3_wait_for_file(self.processing_bucket, filename)

        download_begin = time.time()
        result = self.download(filename)
        download_time = time.time() - download_begin
        reduce_end = time.time()

        bench = AWSBENCH(
            len(ranges),
            round(reduce_begin - invoke_begin, 4),
            round(reduce_end - reduce_begin, 4)
        )

        # Clean up intermediate objects after we're done
        self.aws.clean_s3_prefix(self.processing_bucket, f'output/{prefix}')

        print(f"Benchmark report: {bench}")

        return result

    def create_init_arguments(self, mapper, lambdas_count, prefix):
        """
        Create init arguments for ProcessAndMerge method.
        """

        encoded_mapper = AWSServiceWrapper.encode_object(mapper)
        encoded_headers = AWSServiceWrapper.encode_object(self.paths)
        encoded_prefix = AWSServiceWrapper.encode_object(prefix)
        encoded_lambdas_count = AWSServiceWrapper.encode_object(lambdas_count)

        invoke_worker = functools.partial(
            self.aws.invoke_worker_lambda,
            script=encoded_mapper,
            certs=self.cert.bytes,
            headers=encoded_headers,
            prefix=encoded_prefix,
            file_count=encoded_lambdas_count,
            logger=self.logger
        )

        return invoke_worker

    def work(self, invoke_worker, ranges):
        with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
            futures = []
            for root_range in ranges:
                future = executor.submit(invoke_worker, root_range=root_range)
                futures.append(future)
            return self.wait_on_futures(futures)

    def wait_for_first_results(self, prefix):
        while self.aws.s3_is_empty(self.processing_bucket, f'output/{prefix}'):
            time.sleep(1)

    def reduce(self, invoke_reducer):
        return invoke_reducer()

    def download(self, filename):
        return self.aws.get_and_deserialize_object_from_s3(filename, self.processing_bucket)

    @staticmethod
    def wait_on_futures(futures):
        return [future.result() for future in futures]

    def distribute_unique_paths(self, paths):
        """
        Spark supports sending files to the executors via the
        `SparkContext.addFile` method. This method receives in input the path
        to the file (relative to the path of the current python session). The
        file is initially added to the Spark driver and then sent to the
        workers when they are initialized.

        Args:
            paths (set): A set of paths to files that should be sent to the
                distributed workers.
        """
        pass

    def add_header(self, path: str):
        with open(path, 'r') as f:
            contents = f.read()
            self.paths.append((path, contents))
