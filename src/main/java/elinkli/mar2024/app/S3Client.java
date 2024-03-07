package elinkli.mar2024.app;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

public class S3Client {
    private final String accessKey;
    private final String secretKey;
    private final String bucketName;

    public S3Client(String accessKey, String secretKey, String bucketName) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucketName = bucketName;
    }

    public void copyFilesToDirectory(String sourceDirectory, String destinationDirectory) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(sourceDirectory);

        ListObjectsV2Result listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
        List<S3ObjectSummary> objectSummaries = listObjectsResult.getObjectSummaries();

        for (S3ObjectSummary objectSummary : objectSummaries) {
            String sourceKey = objectSummary.getKey();
            String destinationKey = destinationDirectory + "/" + sourceKey.substring(sourceKey.lastIndexOf("/") + 1);

            if (!s3Client.doesObjectExist(bucketName, sourceKey)) {
                continue;
            }

            if (!s3Client.doesObjectExist(bucketName, destinationKey)) {
                s3Client.copyObject(new CopyObjectRequest(bucketName, sourceKey, bucketName, destinationKey));
            }
        }
    }
}
