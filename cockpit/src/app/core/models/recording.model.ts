/**
 * Recording data models for voice messages in the persistent chat composer.
 */

export interface RecordingConfig {
  mimeType?: string;
  audioConstraints?: MediaTrackConstraints;
}

export interface RecordingResult {
  blob: Blob;
  duration: number;
  mimeType: string;
}

export interface RecordingState {
  isRecording: boolean;
  /** Whole seconds since the recorder started. */
  duration: number;
}

export interface DeviceCapabilities {
  hasCamera: boolean;
  hasMultipleCameras: boolean;
  hasGeolocation: boolean;
  hasAudioInput: boolean;
  isMobile: boolean;
}
