import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { createWriteStream, createReadStream, existsSync, mkdirSync } from 'fs';
import { unlink } from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import ffmpeg from 'fluent-ffmpeg';
import { path as ffmpegPath } from '@ffmpeg-installer/ffmpeg';
import { Logger } from 'winston';
import sharp from 'sharp';
import { Readable } from 'stream';
import { NewsAlertModel } from '../models/news-alert.model';
import * as fs from 'fs';

// Configure FFmpeg path
ffmpeg.setFfmpegPath(ffmpegPath);

export class VideoProcessor {
  private readonly TEMP_DIR: string;

  constructor(
    private readonly s3: S3Client,
    private readonly logger: Logger,
    private readonly rawBucket: string,
    private readonly processedBucket: string,
  ) {
    this.TEMP_DIR = path.join(__dirname, '..', 'trading-app-uploads');
    // Create uploads directory if it doesn't exist
    if (!existsSync(this.TEMP_DIR)) {
      mkdirSync(this.TEMP_DIR, { recursive: true });
      this.logger.info(`Created uploads directory at ${this.TEMP_DIR}`);
    }
  }

  async processMedia(key: string): Promise<void> {
    const { newsAlertId, type } = this.extractMediaInfo(key);
    if (!newsAlertId) {
      throw new Error('Invalid file path format');
    }

    // Get news alert
    const newsAlert = await NewsAlertModel.findById(newsAlertId);
    if (!newsAlert) {
      throw new Error('News alert not found');
    }

    console.log('\nnewsAlert content check', newsAlert);

    // Update status to processing
    await this.updateProcessingStatus(newsAlertId, 'processing');

    console.log('\ntype --------------\n', type);
    try {
      if (type === 'video') {
        await this.processVideoFile(newsAlertId, key);
      } else {
        await this.processImageFiles(newsAlertId);
      }

      // Update status to completed
      await this.updateProcessingStatus(newsAlertId, 'completed');
      this.logger.info(`Successfully processed ${type}: ${key}`);
    } catch (error) {
      this.logger.error(`Error processing ${type}:`, error);
      await this.updateProcessingStatus(newsAlertId, 'failed');
      throw error;
    }
  }

  private extractMediaInfo(key: string): { newsAlertId: string | null; type: 'video' | 'images' } {
    const videoMatch = key.match(/videos\/([^/]+)\/original\.mp4$/);
    if (videoMatch) {
      return { newsAlertId: videoMatch[1], type: 'video' };
    }

    const imageMatch = key.match(/images\/([^/]+)\//);
    if (imageMatch) {
      return { newsAlertId: imageMatch[1], type: 'images' };
    }

    return { newsAlertId: null, type: 'video' };
  }

  private async processVideoFile(newsAlertId: string, key: string): Promise<void> {
    this.logger.info('Processing video file:', { newsAlertId, key });
    
    // If key is a full URL, extract just the path
    if (key.startsWith('http')) {
      const url = new URL(key);
      key = url.pathname.slice(1); // Remove leading slash
      this.logger.info('Extracted key from URL:', { originalKey: key, newKey: key });
    }
    
    const tempFilePath = path.join(this.TEMP_DIR, `${uuidv4()}.mp4`);
    const hlsBasePath = path.join(this.TEMP_DIR, `${uuidv4()}_hls`);
    mkdirSync(hlsBasePath, { recursive: true });

    const processedPaths = {
      '480p': path.join(hlsBasePath, '480p'),
      '720p': path.join(hlsBasePath, '720p'),
      '1080p': path.join(hlsBasePath, '1080p'),
      'thumbnail': path.join(this.TEMP_DIR, `${uuidv4()}_thumb.jpg`)
    };

    // Create directories for each quality
    Object.values(processedPaths).forEach(dir => {
      if (dir !== processedPaths.thumbnail) {
        mkdirSync(dir, { recursive: true });
      }
    });

    this.logger.info('Created temporary paths:', { tempFilePath, processedPaths });

    try {
      // Download from raw bucket
      await this.downloadFromS3(key, tempFilePath);
      this.logger.info('Successfully downloaded video from S3');

      // Process video into different formats
      this.logger.info('Starting video conversion');
      await Promise.all([
        this.convertVideoToHLS(tempFilePath, processedPaths['480p'], 854, 480),
        this.convertVideoToHLS(tempFilePath, processedPaths['720p'], 1280, 720),
        this.convertVideoToHLS(tempFilePath, processedPaths['1080p'], 1920, 1080),
        this.generateThumbnail(tempFilePath, processedPaths['thumbnail'])
      ]);
      this.logger.info('Completed video conversion');

      // Upload processed files
      this.logger.info('Starting upload of HLS streams');
      const urls = await Promise.all([
        this.uploadHLSToS3(processedPaths['480p'], `videos/${newsAlertId}/480p`),
        this.uploadHLSToS3(processedPaths['720p'], `videos/${newsAlertId}/720p`),
        this.uploadHLSToS3(processedPaths['1080p'], `videos/${newsAlertId}/1080p`),
        this.uploadToS3(processedPaths['thumbnail'], `videos/${newsAlertId}/thumbnail.jpg`)
      ]);

      this.logger.info('Completed upload of processed files');

      // Update news alert with processed video URLs
      await NewsAlertModel.findByIdAndUpdate(newsAlertId, {
        $set: {
          'videoFormats.480p': `${urls[0]}/playlist.m3u8`,
          'videoFormats.720p': `${urls[1]}/playlist.m3u8`,
          'videoFormats.1080p': `${urls[2]}/playlist.m3u8`,
          'videoFormats.thumbnail': urls[3]
        }
      });
      this.logger.info('Updated news alert with processed video URLs');
    } catch (error) {
      this.logger.error('Error in processVideoFile:', {
        newsAlertId,
        key,
        error: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : undefined
      });
      throw error;
    } finally {
      // Cleanup temp files
      this.logger.info('Cleaning up temporary files');
      await Promise.all([
        unlink(tempFilePath).catch(err => this.logger.warn('Error deleting temp file:', err)),
        ...Object.values(processedPaths).map(p => 
          p === processedPaths.thumbnail
            ? unlink(p).catch(err => this.logger.warn('Error deleting thumbnail:', err))
            : this.removeDirectory(p).catch(err => this.logger.warn('Error deleting HLS directory:', err))
        )
      ]);
    }
  }

  private async processImageFiles(newsAlertId: string): Promise<void> {
    const newsAlert = await NewsAlertModel.findById(newsAlertId);
    if (!newsAlert?.rawAwsLinkImages?.length) {
      throw new Error('No raw images found');
    }

    const processedImages = await Promise.all(
      newsAlert.rawAwsLinkImages.map(async (rawUrl: string, index: number) => {
        const tempFilePath = path.join(this.TEMP_DIR, `${uuidv4()}.jpg`);
        const processedPaths = {
          original: path.join(this.TEMP_DIR, `${uuidv4()}_original.jpg`),
          thumbnail: path.join(this.TEMP_DIR, `${uuidv4()}_thumb.jpg`),
          medium: path.join(this.TEMP_DIR, `${uuidv4()}_medium.jpg`),
          large: path.join(this.TEMP_DIR, `${uuidv4()}_large.jpg`)
        };

        try {
          // Download from raw bucket
          const key = new URL(rawUrl).pathname.slice(1); // Remove leading slash
          await this.downloadFromS3(key, tempFilePath);

          // Process image into different sizes
          await Promise.all([
            this.processImage(tempFilePath, processedPaths.original, 2560),
            this.processImage(tempFilePath, processedPaths.thumbnail, 150),
            this.processImage(tempFilePath, processedPaths.medium, 800),
            this.processImage(tempFilePath, processedPaths.large, 1920)
          ]);

          // Upload processed files
          const urls = await Promise.all([
            this.uploadToS3(processedPaths.original, `images/${newsAlertId}/${index}/original.jpg`),
            this.uploadToS3(processedPaths.thumbnail, `images/${newsAlertId}/${index}/thumbnail.jpg`),
            this.uploadToS3(processedPaths.medium, `images/${newsAlertId}/${index}/medium.jpg`),
            this.uploadToS3(processedPaths.large, `images/${newsAlertId}/${index}/large.jpg`)
          ]);

          return {
            original: urls[0],
            thumbnail: urls[1],
            medium: urls[2],
            large: urls[3]
          };
        } finally {
          // Cleanup temp files
          await Promise.all([
            unlink(tempFilePath).catch(() => {}),
            ...Object.values(processedPaths).map(p => unlink(p).catch(() => {}))
          ]);
        }
      })
    );

    // Update news alert with processed image URLs
    await NewsAlertModel.findByIdAndUpdate(newsAlertId, {
      $set: { images: processedImages }
    });
  }

  private async downloadFromS3(key: string, filePath: string): Promise<void> {
    this.logger.info(`Downloading from S3: ${this.rawBucket}/${key} to ${filePath}`);
    try {
      const response = await this.s3.send(
        new GetObjectCommand({ Bucket: this.rawBucket, Key: key })
      );

      if (!response.Body) {
        throw new Error('No response body from S3');
      }

      const writeStream = createWriteStream(filePath);
      
      if (response.Body instanceof Readable) {
        this.logger.info('Processing Readable stream from S3');
        await new Promise((resolve, reject) => {
          (response.Body as Readable).pipe(writeStream)
            .on('error', (err) => {
              this.logger.error('Error writing stream:', err);
              reject(err);
            })
            .on('finish', () => {
              this.logger.info('Successfully wrote file to disk');
              resolve(undefined);
            });
        });
      } else {
        // Handle non-Readable response body (e.g., Blob)
        this.logger.info('Processing non-Readable response from S3');
        const chunks = [];
        for await (const chunk of response.Body as ReadableStream) {
          chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);
        await new Promise((resolve, reject) => {
          writeStream.write(buffer, (err) => {
            if (err) {
              this.logger.error('Error writing buffer to disk:', err);
              reject(err);
            } else {
              this.logger.info('Successfully wrote buffer to disk');
              resolve(undefined);
            }
          });
        });
      }
    } catch (error) {
      this.logger.error('Error downloading from S3:', {
        bucket: this.rawBucket,
        key,
        error: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : undefined
      });
      throw error;
    }
  }

  private async convertVideoToHLS(inputPath: string, outputDir: string, width: number, height: number): Promise<void> {
    this.logger.info('Converting video to HLS:', { inputPath, outputDir, width, height });
    await new Promise<void>((resolve, reject) => {
      ffmpeg()
        .input(inputPath)
        .outputOptions([
          '-c:v libx264',
          '-c:a aac',
          '-b:a 128k',
          '-preset medium',
          '-crf 23',
          '-sc_threshold 0',
          '-g 60',
          '-keyint_min 60',
          '-hls_time 6',
          '-hls_playlist_type vod',
          '-hls_segment_filename', path.join(outputDir, 'segment_%03d.ts'),
          '-f hls'
        ])
        .size(`${width}x${height}`)
        .output(path.join(outputDir, 'playlist.m3u8'))
        .on('start', (command) => {
          this.logger.info('Started FFmpeg with command:', command);
        })
        .on('progress', (progress) => {
          this.logger.info('Processing:', progress);
        })
        .on('end', () => {
          this.logger.info('Finished processing HLS stream');
          resolve();
        })
        .on('error', (err: Error) => {
          this.logger.error('FFmpeg error:', err);
          reject(new Error(`FFmpeg error: ${err.message}`));
        })
        .run();
    });
  }

  private async generateThumbnail(inputPath: string, outputPath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      ffmpeg()
        .input(inputPath)
        .outputOptions(['-vframes', '1', '-ss', '00:00:01'])
        .size('640x360')
        .output(outputPath)
        .on('start', (command) => {
          this.logger.info('Started FFmpeg with command:', command);
        })
        .on('end', () => {
          this.logger.info('Finished generating thumbnail');
          resolve();
        })
        .on('error', (err: Error) => {
          this.logger.error('FFmpeg error:', err);
          reject(new Error(`FFmpeg error: ${err.message}`));
        })
        .run();
    });
  }

  private async processImage(inputPath: string, outputPath: string, width: number): Promise<void> {
    this.logger.info('Processing image:', { inputPath, outputPath, width });
    await sharp(inputPath)
      .resize(width, null, { withoutEnlargement: true })
      .jpeg({ quality: 80 })
      .toFile(outputPath);
  }

  private async uploadToS3(filePath: string, key: string): Promise<string> {
    const fileStream = createReadStream(filePath);
    const contentType = key.endsWith('.jpg') ? 'image/jpeg' : 'video/mp4';
    
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.processedBucket,
        Key: key,
        Body: fileStream,
        ContentType: contentType,
        ACL: 'public-read'
      })
    );

    return `https://${this.processedBucket}.s3.amazonaws.com/${key}`;
  }

  private async uploadHLSToS3(dirPath: string, s3KeyPrefix: string): Promise<string> {
    const files = await fs.promises.readdir(dirPath);
    
    // Upload all files in the HLS directory
    await Promise.all(files.map(async (file) => {
      const filePath = path.join(dirPath, file);
      const contentType = file.endsWith('.m3u8') ? 'application/x-mpegURL' : 'video/MP2T';
      const key = `${s3KeyPrefix}/${file}`;
      
      const fileStream = createReadStream(filePath);
      await this.s3.send(
        new PutObjectCommand({
          Bucket: this.processedBucket,
          Key: key,
          Body: fileStream,
          ContentType: contentType,
          ACL: 'public-read'
        })
      );
    }));

    return `https://${this.processedBucket}.s3.amazonaws.com/${s3KeyPrefix}`;
  }

  private async removeDirectory(dir: string): Promise<void> {
    const files = await fs.promises.readdir(dir);
    await Promise.all(
      files.map(file => unlink(path.join(dir, file)))
    );
    await fs.promises.rmdir(dir);
  }

  private async updateProcessingStatus(
    newsAlertId: string,
    status: 'pending' | 'processing' | 'completed' | 'failed'
  ): Promise<void> {
    await NewsAlertModel.findByIdAndUpdate(newsAlertId, {
      $set: { processingStatus: status }
    });
  }
} 