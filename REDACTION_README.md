 # PII Redaction Banner Lambda function
	   
This serverless app helps you redact PII (Personally Identifiable Information) from valid text files present in s3. 
This app deploy a Lambda function which can be attached to banner access point.   
The lambda function internally uses AWS Comprehend to detect pii entities from the text 

## App Architecture
![Architecture Diagram](images/architecture.gif)

Lambda function is optimized to leverage Comprehend's ClassifyPiiDocument and DetectPIIEntities API efficiently to save time and cost for large documents  
ClassifyPiiDocument API is a high-throughput API which acts as a filter so only those documents that contain PII are sent to the DetectPIIEntities API for the compute heavy PII redaction task

1. Lambda function is invoked with a request containing information about the S3 object to get and transform.
2. The request contains a S3 presigned url to fetch the requested object. 
3. The data is split into chunks that are accepted by Comprehend’s ClassifyPiiDocument API and call the API with each chunk.
4. For each chunk which contains PII data, the chunks are split further into smaller chunks with max size supported by Comprehend’s DetectPiiEntities API.
5. For each of these smaller PII chunks, Comprehend’s DetectPIIEntities API is invoked to detect the text spans containing interested PII entities.
6. The responses are aggregated from all chunks.
7. Lambda function callsback S3 with the response i.e the redacted document. 
8. If any failure happens while processing, Lambda function returns an appropriate error response to S3 which will be returned to the original caller.
9. Lambda function returns with 0 exit code .i.e. with out any error if no error occurred else would fail.

## Installation Instructions

1. [Create an AWS account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) if you do not already have one and login
1. Go to the app's page on the [Serverless Application Repository](TODO) 
1. Provide the required app parameters (see parameter details below) and click "Deploy"

## Parameters
Following are the parameters that you can tune to get desired behavior
#### Environment variables
Following environment variables for Lambda function can be set to get desired behaviour  
1. `LOG_LEVEL` (optional) - Log level for Lambda function function logging, e.g., ERROR, INFO, DEBUG, etc. Default: `INFO`
1. `UNSUPPORTED_FILE_HANDLING`(optional) Handling logic for Unsupported files. Valid values are `PASS` and `FAIL`. Default: `PASS`
1. `MAX_DOC_SIZE_PII_CLASSIFICATION` Maximum document size (in bytes) to be used for making calls to Comprehend's ClassifyzPiiDocument API Default: 5000 B
1. `MAX_DOC_SIZE_PII_DETECTION` (optional) : Maximum document size (in bytes) to be used for making calls to Comprehend's DetectPiiEntities API. Default: 5120 i.e. 5KB 
1. `PII_ENTITY_TYPES` (optional) : List of comma separated pii entity types to be considered for redaction. Default: `ALL`
1. `MASK_CHARACTER` (optional) : A character that replaces each character in the redacted PII entity. Default: *
1. `MASK_MODE` (optional) : Specifies whether the PII entity is redacted with the mask character or the entity type. Valid values: `MASK` and `REPLACE_WITH_PII_ENTITY_TYPE`. Default: `MASK`
1. `SUBSEGMENT_OVERLAPPING_TOKENS` (optional) : Number of tokens/words to overlap among segments of a document in case chunking is needed because of maximum document size limit. Default: 20
1. `DEFAULT_MAX_DOC_SIZE` (optional) : Default maximum document size (in bytes) that this function can process otherwise with throw exception for too large document size.
1. `CONFIDENCE_THRESHOLD` (optional) : The minimum prediction confidence score above which pii classification and detection would be considered as final answer. Default: 0
1. `MAX_CHARS_OVERLAP` (optional) : Maximum characters to overlap among segments of a document in case chunking is needed because of maximum document size limit. Default: 2
1. `DEFAULT_LANGUAGE_CODE` (optional) : Default language of the text to be processed. This code will be used for interacting with Comprehend . Default: en
1. `DETECT_PII_ENTITIES_THREAD_COUNT` (optional) : Number of threads to use for calling Comprehend's DetectPiiEntities API. This controls the number of simultaneous calls that will be made from this Lambda function. Default: 5
1. `CLASSIFY_PII_DOC_THREAD_COUNT` (optional) : Number of threads to use for calling Comprehend's ClassifyPiiDocument API. This controls the number of simultaneous calls tha will be made from this Lambda function. Default: 2
1. `REDACTION_API_ONLY` (optional) : This determines whether or not to only use DetectPiiEntities or use ClassifyPiiDocument first. Default: true
1. `PUBLISH_CLOUD_WATCH_METRICS` (optional) : This determines whether or not to publish metrics to Cloudwatch. Default: true

#### Runtime variables
You can add following arguments in banner access point configuration payload to override the default value configured used by the Lambda function . These values would take precedence over environment variables.
Use these parameters to get desired behaviors from different access point configuration attached to the same lambda function     
1. `pii_entity_types` : List of pii entity types to be considered for redaction. e.g.  `["SSN","CREDIT_DEBIT_NUMBER"]`
1. `mask_mode` : Specifies whether the PII entity is redacted with the mask character or the entity type. Valid values: `MASK` and `REPLACE_WITH_PII_ENTITY_TYPE`.  
1. `mask_character` : A character that replaces each character in the redacted PII entity.
1. `confidence_threshold` :The minimum prediction confidence score above which pii classification and detection would be considered as final answer
1. `language_code`: Language of the text. This will be used to interact with Comprehend 

## App Outputs

#### Successful response
In case the text file contains PII, it would be redacted and returned in response to GetObject API output  
#### Error responses
Lambda function would forward the standard [s3 error responses](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html) it will receive while downloading the file from s3
  
Further following error responses will be thrown by Lambda function:

|Status Code|Error Code|Error Message|Description|
|---|---|---|---|
| BAD_REQUEST_400 | InvalidRequest | Lambda function has been incorrectly setup | An incorrect configuration which restricts lambda function to even start handling the incoming events|
| PRECONDITION_FAILED_412 | PreconditionFailed |  Unsupported file encountered for determining Pii | This error would be thrown in case caller tries to get an invalid utf8 file (e.g image) and UNSUPPORTED_FILE_HANDLING variable is set to FAIL|  
| PRECONDITION_FAILED_412 | EntityTooLarge | Size of the requested object exceeds maximum file size supported  | This error would be thrown in case caller tries to get an object which is beyond the max file size supported|   
| INTERNAL_SERVER_ERROR_500 | InternalError | An internal error occurred while processing the file | Any other error occurred while processing the object |   

## Metrics
Metrics are published after each invocation of the lambda function and are a best effort attempt (Failures in CloudWatch metric publishing are ignored)

All metrics will be under the Namespace: BannerLambda

### Metrics for Banner Lambda
|MetricName|Description|Unit|Dimensions|
|---|---|---|---|
|PiiDocumentsProcessed|Emitted after processing a document that contains pii|Count|BannerAccessPoint, Language|
|DocumentsProcessed|Emitted after processing any document|Count|BannerAccessPoint, Language|
|PiiDocumentTypesProcessed|Emitted after processing a document that contains pii for each type of pii of interest|Count|BannerAccessPoint, Language, PiiEntityType|

### Metrics for Comprehend APIs
|MetricName|Description|Unit|Dimensions|
|---|---|---|---|
|Latency|The latency of Comprehend DetectPiiEntities API|Milliseconds|Comprehend, DetectPiiEntities|
|Latency|The latency of Comprehend ClassifyPiiDocument API|Milliseconds|Comprehend, ClassifyPiiDocument|
|FaultCount|The fault count of Comprehend DetectPiiEntities API|Count|Comprehend, DetectPiiEntities|
|FaultCount|The fault count of Comprehend ClassifyPiiDocument API|Count|Comprehend, ClassifyPiiDocument|

### Metrics for S3 APIs
|MetricName|Description|Unit|Dimensions|
|---|---|---|---|
|Latency|The latency of S3 WriteGetObjectResponse API|Milliseconds|S3, WriteGetObjectResponse|
|Latency|The latency of downloading a file from a presigned S3 url|Milliseconds|S3, DownloadPresignedUrl|
|FaultCount|The fault count of S3 WriteGetObjectResponse API|Count|S3, DetectPiiEntities|
|FaultCount|The fault count of downloading a file from a presigned S3 url|Count|S3, DownloadPresignedUrl|

## License Summary

This code is made available under the MIT-0 license. See the LICENSE file.