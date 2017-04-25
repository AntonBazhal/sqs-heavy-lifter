'use strict';

const AWS = require('aws-sdk');
const uuid = require('uuid');

const MAX_MESSAGE_SIZE = 262144; // 256 KB
const MAX_MESSAGE_ATTRIBUTES = 10;

function getStringSizeInBytes(value) {
	return Buffer.byteLength(value || '', 'utf8');
}

// Binary value can be Buffer, Typed Array, Blob, String
// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS.html#sendMessage-property
function getBinarySizeInBytes(value) {
	if (!value) {
		return 0;
	}

	if (typeof value === 'string') {
		return getStringSizeInBytes(value);
	}

	return value.byteLength // TypedArray
		|| value.length // Buffer
		|| value.size; // Blob
}

function getMessageAttributesSize(messageAttributes) {
	return Object.keys(messageAttributes).reduce((acc, attrName) => {
		const attr = messageAttributes[attrName];

		return acc
			+ getStringSizeInBytes(attrName)
			+ getStringSizeInBytes(attr.DataType)
			+ getStringSizeInBytes(attr.StringValue)
			+ getBinarySizeInBytes(attr.BinaryValue);
	}, 0);
}

function getMessageSize(message) {
	return getStringSizeInBytes(JSON.stringify(message));
}

function getTotalMessageSize(message, messageAttributes) {
	return getMessageSize(message) + getMessageAttributesSize(messageAttributes);
}

class SqsHeavyLifter {

	constructor(options) {
		options = options || {}; // eslint-disable-line no-param-reassign

		if (!options.queueUrl) {
			throw new Error('queueUrl is required');
		}

		if (options.bucketName && typeof options.bucketName !== 'string') {
			throw new Error('bucketName must be a string');
		}

		if (options.maxMessageSize &&
			(typeof options.maxMessageSize !== 'number' || options.maxMessageSize <= 0)) {
			throw new Error('maxMessageSize must be a positive number');
		}

		this.queueUrl = options.queueUrl;
		this.bucketName = options.bucketName;
		this.alwaysS3 = options.alwaysS3;
		this.maxMessageSize = options.maxMessageSize || MAX_MESSAGE_SIZE;
		this._sqsClient = new AWS.SQS(options.awsSQS || options.aws || {});
		this._s3Client = new AWS.S3(options.awsS3 || options.aws || {});
	}

	_sendThroughS3(message, messageAttributes) {
		const objectKey = uuid.v4();

		return this._s3Client
			.putObject({
				Bucket: this.bucketName,
				Key: objectKey,
				Body: JSON.stringify(message)
			})
			.then(() => {
				const sqsMessage = {
					s3BucketName: this.bucketName,
					s3Key: objectKey
				};
				return this._sendToSQS(sqsMessage, messageAttributes);
			});
	}

	_sendToSQS(message, messageAttributes) {
		return this._sqsClient.sendMessage({
			QueueUrl: this.queueUrl,
			MessageBody: JSON.stringify(message),
			MessageAttributes: messageAttributes
		}).promise();
	}

	sendMessage(message, messageAttributes) {
		if (!message) {
			throw new Error('message is required');
		}

		if (messageAttributes && Object.keys(messageAttributes).length > MAX_MESSAGE_ATTRIBUTES) {
			throw new Error(`messageAttributes may contain up to ${MAX_MESSAGE_ATTRIBUTES} attributes`);
		}

		if (this.alwaysS3 || getTotalMessageSize(message, messageAttributes) > this.maxMessageSize) {
			return this._sendThroughS3(message, messageAttributes);
		}

		return this._sendToSQS(message, messageAttributes);
	}
}

module.exports = SqsHeavyLifter;
