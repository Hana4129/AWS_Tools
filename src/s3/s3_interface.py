# -*- coding: utf-8 -*-

import boto3
import os

class S3FilesProcessing:

    def __init__(self):
        """
            Class initial...
        """
        self.s3 = boto3.resource('s3')

    def __get_all_keys(self, bucket:str, prefix:str, recursive: bool = False, keys: list = [], marker: str=''):
        """
            get directory all keys. 

        Args:
            bucket: S3 Target Bucket Name.
            prefix: S3 Target Directory Name.
            recursive: recursive directorys.
            keys: 全パス取得用変数。list_objects の戻り値 Contents keyを保存.


        Returns:
            list: The return value. contain S3 filenames.

        """

        # get bucket
        bucket = self.s3.Bucket(bucket)

        if recursive:
            response = self.s3.list_objects(
                Bucket=bucket, Prefix=prefix, Marker=marker)
        else:
            response = self.s3.list_objects(
                Bucket=bucket, Prefix=prefix, Marker=marker, Delimiter='/')

        # 指定ディレクトリ配下のみ場合
        if 'CommonPrefixes' in response:
            keys.extend([content['Prefix']
                        for content in response['CommonPrefixes']])


        if 'Contents' in response:  
            keys.extend([content['Key'] for content in response['Contents']])
            # 取得結果が1000件超で取得残あり
            if 'IsTruncated' in response:
                return self.__get_all_keys(bucket=bucket, prefix=prefix, recursive=recursive, keys=keys, marker=keys[-1])
        return keys

    def getList(self, bucket:str, prefix:str, recursive: bool = False):
        """
            get directory object list.

        Args:
            bucket: S3 Target Bucket Name.
            prefix: S3 Target Directory Name.
            recursive: recursive directorys.

        Returns:
            list: The return value. contain S3 filenames.

        """

        pathList = []
        # path search.
        pathList= self.__get_all_keys(
                bucket, prefix, recursive=recursive)

    def rename(self, bucket:str, oldPath:str, newPath:str):
        """
            rename s3 file.

        Args:
            bucket: S3 Target Bucket Name.
            oldPath: Old File Path.
            newPath: New File Path.
       
        """
        # ファイルをリネーム
        copysrc = os.path.join(bucket,oldPath.lstrip("/"))
        self.s3.Object(bucket,newPath).copy_from(CopySource=copysrc)
        self.s3.Object(bucket,oldPath).delete()

    def upload(self, bucket:str, localPath:str, uploadPath:str):
        """
            upload s3.

        Args:
            bucket: S3 Target Bucket Name.
            localPath: Local Filepath.
            uploadPath: uploadPath.

        """

        # get bucket
        bucket = self.s3.Bucket(bucket)
        bucket.upload_file(localPath, uploadPath)


    def download(self, bucket:str, downloadPath:str, localPath:str):
        """
            download s3.

        Args:
            bucket: S3 Target Bucket Name.
            downloadPath: s3 Download Path.
            localPath: Local Filepath.


        """

        # get bucket
        bucket = self.s3.Bucket(bucket)
        bucket.download_file(downloadPath, localPath)

