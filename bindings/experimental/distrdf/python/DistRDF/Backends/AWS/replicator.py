import logging
import functools
import os
import uuid
import random
import time

from collections import namedtuple

from .AWS_utils import AWSServiceWrapper
from .reducer import Reducer
from .krbcert import KRBCert
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import ROOT


ReplicateRange = namedtuple("ReplicateRange", ["start", "length", "source", "destination", "part_number", "id"])
MB = 1024 ** 2

logger = logging.getLogger('replicator')


class Replicator:
    REPLICATION_NAMESPACE = uuid.UUID('98f862dd-1fab-5adf-8638-7f5a665bad8c')
    REPLICATE_BUFFSIZE = 100 * MB

    def __init__(self, bucket, region, lambdas_count):
        self.bucket: str = bucket
        self.aws = AWSServiceWrapper(region)
        self.cert = KRBCert()
        self.lambdas_count: int = lambdas_count

    def replicate(self, files: list[str]) -> list[str]:
        """
        Replicate files, which are not yet replicated
        and returns urls to replicated ones.
        """
        newfiles, to_replicate = self.process_files(files)

        if to_replicate:
            logger.debug('Replicating...')
            before = time.time()
            self.replicate_files(to_replicate)
            after = time.time()
            logger.debug('Replicating finished')
            print(f'replicate_time={after - before}')

        return newfiles
        
        
    def replicate_files(self, to_replicate: list[tuple[str, str]]):
        ranges, first_ranges = self.create_replicate_ranges(to_replicate)

        logger.debug('Ranges created')
        for rr in first_ranges:
            logger.debug(f'RepRange(uid={rr.id}, src={rr.source}, dst={rr.destination})')

        # For serialization
        ranges = [tuple(r) for r in ranges]
        chunks = self.shuffle_and_partition(ranges, self.lambdas_count)
        logger.debug(f'Chunked. Chunk approx size: {len(chunks[0])}')

        self.invoke_replicating_lambdas(chunks, self.lambdas_count)
        self.finish_uploads(first_ranges)

    def process_files(self, files: list[str]):
        newfiles = []
        to_replicate = []

        for src_filename in files:
            parsed = urlparse(src_filename)
            dst_filename = self.get_replicated_filename(parsed.path)
            newurl = self.get_replicated_url(dst_filename)
            newfiles.append(newurl)

            if (parsed.scheme != 's3' and
                    not self.aws.s3_object_exists(self.bucket, dst_filename)):
                to_replicate.append((src_filename, dst_filename))

        return newfiles, to_replicate

    def get_replicated_filename(self, path: str, prefix='input'):
        hashed_path = uuid.uuid5(self.REPLICATION_NAMESPACE, path)
        basename = os.path.basename(path)
        name = f'{str(hashed_path)}__{basename}'
        return '/'.join([prefix, name])

    def get_replicated_url(self, filename: str) -> str:
        return f's3://{self.bucket}.s3.{self.aws.region}.amazonaws.com/{filename}'

    def create_replicate_ranges(self, to_replicate: list[tuple[str]]):
        ranges = []
        first_ranges = []
        for src_filename, dst_filename in to_replicate:
            file_ranges = self.create_replicate_ranges_batch(
                src_filename,
                dst_filename
            )
            ranges.extend(file_ranges)
            first_ranges.append(file_ranges[0])

        return ranges, first_ranges

    def create_replicate_ranges_batch(
        self,
        src_filename: str,
        dst_filename: str
    ) -> list[ReplicateRange]:
        src_filesize = self.get_rootfile_size(src_filename)
        buff_size = self.adjust_buffsize(src_filesize)
        upload_id = self.aws.start_multipart_upload(
            self.bucket,
            dst_filename
        )

        fileleft = src_filesize
        ranges = []
        idx = 1
        for i in range(0, src_filesize, buff_size):
            replicate_range = ReplicateRange(
                start=i,
                length=buff_size if fileleft >= buff_size else fileleft,
                source=src_filename,
                destination=dst_filename,
                part_number=idx,
                id=upload_id
            )
            idx += 1
            fileleft -= buff_size
            ranges.append(replicate_range)

        return ranges

    @staticmethod
    def get_rootfile_size(filename: str) -> int:
        f = ROOT.TFile.Open(filename)
        size = f.GetSize()
        f.Close()
        return size

    def adjust_buffsize(self, filesize: int) -> int:
        """
        AWS multipart upload allows only 10000 parts.
        Adjust buffer size, so that it is guaranteed
        there will be no more than 10000 parts.
        """
        return max(self.REPLICATE_BUFFSIZE, filesize // 9999)

    def shuffle_and_partition(
        self,
        ranges: list[ReplicateRange],
        parts_count: int
    ) -> list[tuple[ReplicateRange]]:
        random.shuffle(ranges)
        return self.partition(ranges, parts_count=parts_count)

    def partition(
        self,
        ranges: list[ReplicateRange],
        parts_count: int
    ) -> list[tuple[ReplicateRange]]:
        chunk_size, leftover = divmod(len(ranges), parts_count)
        evenly_dividable_count = chunk_size * parts_count
        evenly_dividable = ranges[:evenly_dividable_count]
        chunked = Reducer.divide_into_chunks(evenly_dividable, chunk_size)

        for i in range(leftover):
            chunked[i] += (ranges[evenly_dividable_count + i],)

        return chunked

    def invoke_replicating_lambdas(self, chunks):
        invoke = functools.partial(
            self.aws.invoke_replicate_lambda,
            certs=self.cert.bytes
        )
        logger.debug('Invoking lambdas...')
        with ThreadPoolExecutor(max_workers=self.lambdas_count) as executor:
            futures = []
            for rranges in chunks:
                future = executor.submit(invoke, ranges=rranges)
                futures.append(future)
            for future in tqdm(as_completed(futures), total=self.lambdas_count):
                pass
        logger.debug('Lambdas finished...')

    @staticmethod
    def wait_on_futures(futures):
        _ = [future.result() for future in futures]

    def finish_uploads(self, first_ranges: list[ReplicateRange]):
        for rrange in first_ranges:
            parts = self.get_upload_parts(rrange.id, rrange.destination)
            
            self.aws.finish_multipart_upload(
                bucket=self.bucket,
                filename=rrange.destination,
                parts=parts,
                uid=rrange.id
            )
        logger.debug('Files upload end')

    def get_upload_parts(self, uid: str, destination: str) -> list:
        parts = self.aws.list_all_parts(
            Bucket=self.bucket,
            Key=destination,
            UploadId=uid
        )
        parts = [self.get_formatted_part(part) for part in parts]
        return sorted(parts, key=lambda x: x['PartNumber'])
        
        
    @staticmethod
    def get_formatted_part(part):
        return {
            'ETag': part['ETag'],
            'PartNumber': part['PartNumber'],
        }

