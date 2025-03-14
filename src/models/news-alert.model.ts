import { Schema, model, Document } from 'mongoose';

interface VideoFormats {
  '480p'?: string;
  '720p'?: string;
  '1080p'?: string;
  thumbnail?: string;
}

interface ImageFormat {
  original: string;
  thumbnail: string;
  medium: string;
  large: string;
}

interface NewsAlert extends Document {
  type: 'video' | 'images';
  videoFormats?: VideoFormats;
  images?: ImageFormat[];
  rawAwsLinkVideo?: string;
  rawAwsLinkImages?: string[];
  processingStatus: 'pending' | 'processing' | 'completed' | 'failed';
  createdAt: Date;
  updatedAt: Date;
}

const newsAlertSchema = new Schema<NewsAlert>(
  {
    type: {
      type: String,
      enum: ['video', 'images'],
      required: true
    },
    videoFormats: {
      '480p': String,
      '720p': String,
      '1080p': String,
      thumbnail: String
    },
    images: [{
      original: String,
      thumbnail: String,
      medium: String,
      large: String
    }],
    rawAwsLinkVideo: String,
    rawAwsLinkImages: [String],
    processingStatus: {
      type: String,
      enum: ['pending', 'processing', 'completed', 'failed'],
      default: 'pending',
    },
  },
  { timestamps: true }
);

export const NewsAlertModel = model<NewsAlert>('NewsAlert', newsAlertSchema); 