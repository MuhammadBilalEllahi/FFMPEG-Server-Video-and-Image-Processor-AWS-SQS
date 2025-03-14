import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { config } from 'dotenv';
import * as mongoose from 'mongoose';
import { createLogger, format, transports } from 'winston';
import { VideoProcessor } from './services/video-processor';

// Load environment variables
config();

// Configure logger
const logger = createLogger({
  format: format.combine(
    format.timestamp(),
    format.json()
  ),
  transports: [
    new transports.Console(),
    new transports.File({ filename: 'error.log', level: 'error' }),
    new transports.File({ filename: 'combined.log' })
  ]
});

// AWS Clients
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  }
});

const sqs = new SQSClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  }
});

// Initialize video processor
const videoProcessor = new VideoProcessor(
  s3,
  logger,
  process.env.AWS_RAW_BUCKET!,
  process.env.AWS_PROCESSED_BUCKET!
);

// Connect to MongoDB
mongoose.connect(process.env.MONGO_DB_URI!)
  .then(() => logger.info('Connected to MongoDB'))
  .catch(err => logger.error('MongoDB connection error:', err));

// Process SQS messages
async function processMessage(message: any): Promise<void> {
  try {
    const body = message.Body;
    if (!body) throw new Error('Empty message body');
    
    const s3Event = JSON.parse(body);
    if (!s3Event.Records?.[0]?.s3) {
      throw new Error('Invalid S3 event format');
    }

    const record = s3Event.Records[0];
    const key = decodeURIComponent(record.s3.object.key);
    
    logger.info(`Processing media: ${key}`);
    await videoProcessor.processMedia(key);
  } catch (error) {
    logger.error('Error processing message:', error);
    throw error;
  }
}

// Main processing loop
async function startProcessing(): Promise<void> {
  logger.info('Starting video processing service...');

  while (true) {
    try {
      const result = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: process.env.AWS_SQS_QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20
      }));

      if (result.Messages && result.Messages.length > 0) {
        const message = result.Messages[0];
        
        try {
          await processMessage(message);
          
          // Delete message after successful processing
          await sqs.send(new DeleteMessageCommand({
            QueueUrl: process.env.AWS_SQS_QUEUE_URL!,
            ReceiptHandle: message.ReceiptHandle!
          }));
        } catch (error) {
          logger.error('Failed to process message:', error);
          // Don't delete the message, it will return to the queue
        }
      }
    } catch (error) {
      logger.error('Error in processing loop:', error);
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

// Start the service
startProcessing().catch(error => {
  logger.error('Fatal error:', error);
  // process.exit(1);
}); 