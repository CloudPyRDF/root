import logging
import functools
import time

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
        self.aws_service_wrapper = AWSServiceWrapper(region_name)
        self.processing_bucket = self.aws_service_wrapper.get_ssm_parameter_value('processing_bucket')

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
            headnode = self.replicated_headnode(headnode)

        return DataFrame.RDataFrame(headnode, self)

    def replicated_headnode(self, headnode: HeadNode):
        if not hasattr(headnode, 'tree'):
            raise ValueError('No file to replicate')

        files = [f.GetTitle() for f in headnode.tree.GetListOfFiles()]
        replicator = Replicator(self.processing_bucket, self.aws_service_wrapper.region, lambdas_count=16)
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

        invoke_func, download_func = self.create_init_arguments(mapper)

        self.logger.info(f'Before lambdas invoke. Number of lambdas: {len(ranges)}')

        invoke_begin = time.time()

        files = self.invoke_and_download(invoke_func, download_func, ranges)

        self.logger.info(f'Lambdas finished.')
        reduce_begin = time.time()

        result = Reducer.tree_reduce(reducer, files)

        bench = AWSBENCH(
            len(ranges),
            round(reduce_begin - invoke_begin, 4),
            round(time.time() - reduce_begin, 4)
        )

        # Clean up intermediate objects after we're done
        self.aws_service_wrapper.clean_s3_prefix(self.processing_bucket, 'output')

        print(f"Benchmark report: {bench}")

        return result

    def create_init_arguments(self, mapper):
        """
        Create init arguments for ProcessAndMerge method.
        """

        pickled_mapper = AWSServiceWrapper.encode_object(mapper)
        pickled_headers = AWSServiceWrapper.encode_object(self.paths)

        invoke_lambda = functools.partial(
            self.aws_service_wrapper.invoke_root_lambda,
            script=pickled_mapper,
            certs=self.cert.bytes,
            headers=pickled_headers,
            logger=self.logger)

        download_partial = functools.partial(
            self.aws_service_wrapper.get_partial_result_from_s3,
            bucket_name=self.processing_bucket)

        return invoke_lambda, download_partial

    def invoke_and_download(self, invoke, download, ranges):
        s3_objects = []
        with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
            futures = []
            for root_range in ranges:
                future = executor.submit(invoke, root_range=root_range)
                futures.append(future)
            s3_objects = [future.result() for future in futures]
        files = []
        before = time.time()
        with ThreadPoolExecutor(max_workers=min(len(s3_objects), 256)) as executor:
            futures = []
            for filename in s3_objects:
                future = executor.submit(download, filename)
                futures.append(future)
            files = [future.result() for future in futures]
        after = time.time()
        print(f'download_time={after - before}')

        if len(files) < len(ranges):
            raise Exception(f'Some lambdas failed after multiple retrials')

        return files

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
