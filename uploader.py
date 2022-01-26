import os
import sys
import boto3
from boto3.s3.transfer import TransferConfig
import botocore
import logging
from multiprocessing import Pool
global client
client = None

# I had this damn thing all nice and encapsulated, but pools can't deal with objects womp womp


def _upload_file(root, bucket, file):
    """ Upload a file to S3 where the keyname is the path
    relative to a root
    param file file to upload
    """
    key = os.path.relpath(file, root)
    logging.debug("Uploading "+file+" as "+key+" to "+bucket)
    client.upload_file(file, bucket, key, Config=config)


class S3Uploader:
    def __init__(self, profile, root, bucket):
        self.profile = profile
        self.root = root
        self.bucket = bucket
        self.error_count = 0

    def _get_files_in_path(self, path):
        """ Get list of files in a path
        """
        ret = []
        for root, dirs, files in os.walk(path):
            for file in files:
                ret.append(os.path.join(root, file))
        return ret

    def upload(self, threads=8):
        """ Upload all files in path
        param threads number of threads to use
        """
        global client
        global config
        # let's make the cutoff for multipart files 4G
        config = TransferConfig(multipart_threshold=(
            4*1024**3), multipart_chunksize=((1024**3)), max_concurrency=8)
        client = boto3.Session(profile_name=self.profile).client('s3')
        # This could be big-ish, but like assuming 255 character paths,
        # maximum character width (we won't see this), and 1M files
        # it should only be like 8G or so.
        # I think a more realistic size is probably 1G.
        files = self._get_files_in_path(self.root)
        logging.info("Beginning upload of "+str(len(files)) +
                     " files to "+self.bucket+".")

        # multi part uploads are by default I think 10 threads, so I'm just
        # gonna arbitrarily decide that if we don't have at least 10/thread
        # files that it's not worth it to parallelize at a files level
        if len(files) > 10*threads:
            results = []
            with Pool(processes=threads) as pool:
                for file in files:
                    results.append(pool.apply_async(
                        _upload_file, args=(self.root, self.bucket, file)))
                pool.close()
                pool.join()
            for result in results:
                try:
                    result.get()
                except Exception as error:
                    self.error_count += 1
                    logging.error('Upload of '+file+' failed.')
                    logging.exception(error)
        else:
            for file in files:
                try:
                    _upload_file(client, self.root, self.bucket, file)
                except Exception as error:
                    self.error_count += 1
                    logging.error('Upload of '+file+' failed.')
                    logging.exception(error)


def main():
    PROFILE = 's3uploader-test'
    ROOT = '/home/snehring/tmp/upload-test'
    BUCKET = 'rit-upload-test'

    log_level = os.getenv('LOGGING')
    if log_level is None:
        log_level = 'INFO'
    logging.getLogger().setLevel(log_level)

    if os.getenv('PROFILE') is not None:
        PROFILE = os.getenv('PROFILE')
    if os.getenv('ROOT') is not None:
        ROOT = os.getenv('ROOT')
    if os.getenv('BUCKET') is not None:
        BUCKET = os.getenv('BUCKET')
    if PROFILE is None or ROOT is None or BUCKET is None:
        logging.error("PROFILE, ROOT, or BUCKET not set.")
        return -1
    uploader = S3Uploader(PROFILE, ROOT, BUCKET)
    uploader.upload()
    if uploader.error_count > 0:
        # errors were encountered
        logging.warning("Some files failed to upload. Please review logs.")


if __name__ == '__main__':
    sys.exit(main())
