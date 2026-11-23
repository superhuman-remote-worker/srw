/**
 * File upload models for job creation.
 */

/**
 * File type categories.
 */
export enum FileType {
  IMAGE = 'image',
  VIDEO = 'video',
  AUDIO = 'audio',
  DOCUMENT = 'document',
  OTHER = 'other',
}

/**
 * Upload status for tracking file upload progress.
 */
export enum UploadStatus {
  PENDING = 'pending',
  UPLOADING = 'uploading',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

/**
 * File preview for local UI state.
 * Tracks file selection, validation, and upload progress.
 */
export interface FilePreview {
  /** Unique local identifier */
  id: string;
  /** Original File object (for upload) */
  file: File;
  /** Display name */
  name: string;
  /** File size in bytes */
  size: number;
  /** Formatted file size (e.g., "1.5 MB") */
  sizeFormatted: string;
  /** Detected file type category */
  type: FileType;
  /** MIME type */
  mimeType: string;
  /** Base64 data URL for image preview */
  preview?: string;
  /** Upload progress percentage (0-100) */
  uploadProgress?: number;
  /**
   * Current upload status. Job creation drives this through its own upload
   * (job-create.component.ts). The chat composer no longer does: a chat
   * attachment's upload state lives on the outbox item that carries it
   * (`PendingUpload.status`), because the file leaves the composer the instant
   * the user sends. `remoteName`/`uploadedFiles` lived here for the same
   * reason and were removed with it.
   */
  uploadStatus: UploadStatus;
  /** Error message if upload failed */
  error?: string;
  /**
   * A dictation recording whose transcript is in the composer text. It
   * uploads to `uploads/` with the message and the agent is told where it is,
   * but it is never shown as a chip (dictation_composer_redesign.md D1).
   */
  dictation?: boolean;
}

/**
 * A file `createFilePreviews()` declined to include in the batch, and why —
 * so the caller can tell the user instead of the file silently vanishing
 * (the bug this type exists to close: see file-handling.service.ts).
 */
export interface RejectedFile {
  /** Original filename, for display in the rejection message. */
  name: string;
  /** 'size': over the per-file byte cap. 'count': over the per-batch file cap. */
  reason: 'size' | 'count';
}

/** Result of `createFilePreviews()`: what was accepted, and what wasn't. */
export interface FilePreviewResult {
  previews: FilePreview[];
  rejected: RejectedFile[];
}

/**
 * Uploaded file metadata from server.
 */
export interface UploadedFile {
  /** Filename as stored on server */
  name: string;
  /** File size in bytes */
  size: number;
  /** MIME type */
  mime_type: string;
}

/**
 * Response from POST /api/uploads endpoint.
 */
export interface UploadResponse {
  /** Unique upload identifier */
  upload_id: string;
  /** List of uploaded files */
  files: UploadedFile[];
}

/**
 * Information about an existing upload.
 */
export interface UploadInfo {
  /** Unique upload identifier */
  upload_id: string;
  /** List of files in the upload */
  files: UploadedFile[];
  /** ISO timestamp when upload was created */
  created_at: string;
}

/**
 * One file pushed into a thread workspace's uploads/ directory.
 */
export interface ThreadUploadedFile {
  name: string;
  size: number;
  mime_type: string;
  path: string;
}

/**
 * Response from POST /api/persistent/threads/{thread_id}/uploads.
 */
export interface ThreadUploadResponse {
  thread_id: string;
  files: ThreadUploadedFile[];
}

/**
 * What `ApiService.uploadOneToThread` emits: zero or more `progress` events
 * while the bytes move, then exactly one `done` carrying the server's entries.
 *
 * `total` is `null` whenever the browser cannot compute the request body
 * length — `HttpUploadProgressEvent.total` is optional, so a consumer that
 * divides by it unguarded renders NaN/Infinity. Indeterminate is a real state.
 */
export type ThreadUploadEvent =
  | {kind: 'progress'; loaded: number; total: number | null}
  | {kind: 'done'; files: ThreadUploadedFile[]};
