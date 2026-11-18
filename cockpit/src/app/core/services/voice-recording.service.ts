import { Injectable, NgZone } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { VoiceAudioProcessor } from '../audio/voice-audio-processor';
import { WaveformVisualizer } from '../audio/waveform-visualizer';
import { RecordingConfig, RecordingResult, RecordingState } from '../models/recording.model';

/**
 * Microphone recording for composer dictation, with a live waveform.
 *
 * Owns the MediaStream/MediaRecorder and an AudioContext for level analysis.
 * The waveform canvas is attached separately (`attachVisualizer`) because the
 * composer only renders it once `isRecording` is true, which is after
 * `startRecording` has resolved. Handing the canvas to `startRecording` meant
 * the service always received `undefined` and no waveform ever ran.
 */
@Injectable({ providedIn: 'root' })
export class VoiceRecordingService {
  private mediaRecorder: MediaRecorder | null = null;
  private audioStream: MediaStream | null = null;
  private audioChunks: Blob[] = [];
  private recordingState$ = new BehaviorSubject<RecordingState>({
    isRecording: false,
    duration: 0,
  });
  private recordingStartTime = 0;
  private recordingTimer: ReturnType<typeof setInterval> | null = null;
  private audioContext: AudioContext | null = null;
  private audioProcessor: VoiceAudioProcessor | null = null;
  private visualizer: WaveformVisualizer | null = null;
  private canvas: HTMLCanvasElement | null = null;
  /** Bumped by every start and cancel: a start whose permission prompt
   *  resolves after a cancel (or a second start) must not take the mic. */
  private startGeneration = 0;
  private starting = false;

  constructor(private ngZone: NgZone) {}

  getRecordingState(): Observable<RecordingState> {
    return this.recordingState$.asObservable();
  }

  /** True between the tap and the recorder running (permission prompt). */
  isStarting(): boolean {
    return this.starting;
  }

  async startRecording(config: RecordingConfig = {}): Promise<void> {
    // A double tap must not open a second stream: the first one would keep
    // the microphone (and its indicator) on with nothing left to stop it.
    if (this.starting || this.recordingState$.value.isRecording) return;
    this.starting = true;
    const generation = ++this.startGeneration;
    // Create the AudioContext inside the tap. Created after the permission
    // await, mobile browsers may start it suspended (no user activation left),
    // and a suspended analyser reads silence: a flat waveform.
    this.createAudioContext();
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: config.audioConstraints ?? {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });
      if (generation !== this.startGeneration) {
        stream.getTracks().forEach((t) => t.stop());
        return;
      }
      this.audioStream = stream;
      this.recordingStartTime = Date.now();
      this.audioChunks = [];
      this.connectAnalyser(stream);

      const mimeType = config.mimeType || this.getSupportedMimeType();
      this.mediaRecorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
      this.mediaRecorder.ondataavailable = (event) => {
        if (event.data.size > 0) this.audioChunks.push(event.data);
      };
      this.mediaRecorder.start(100);

      this.startRecordingTimer();
      this.recordingState$.next({ isRecording: true, duration: 0 });
      this.startVisualizer();
    } catch (error) {
      console.error('Error starting recording:', error);
      if (generation === this.startGeneration) this.cleanup();
      throw error;
    } finally {
      if (generation === this.startGeneration) this.starting = false;
    }
  }

  /** Draw the waveform on `canvas` (the composer calls this once it renders). */
  attachVisualizer(canvas: HTMLCanvasElement): void {
    if (this.canvas === canvas) return;
    this.visualizer?.destroy();
    this.visualizer = null;
    this.canvas = canvas;
    this.startVisualizer();
  }

  /** The canvas left the DOM. */
  detachVisualizer(): void {
    this.visualizer?.destroy();
    this.visualizer = null;
    this.canvas = null;
  }

  async stopRecording(): Promise<RecordingResult | null> {
    const recorder = this.mediaRecorder;
    if (!recorder || !this.recordingState$.value.isRecording) {
      return null;
    }

    return new Promise<RecordingResult>((resolve) => {
      recorder.onstop = () => {
        const duration = this.recordingState$.value.duration;
        const mimeType = recorder.mimeType || 'audio/webm';
        const blob = new Blob(this.audioChunks, { type: mimeType });
        this.cleanup();
        resolve({ blob, duration, mimeType });
      };
      recorder.stop();
    });
  }

  /** Abandon a recording, or a start still waiting on the permission prompt. */
  cancelRecording(): void {
    this.startGeneration++;
    this.starting = false;
    if (this.mediaRecorder && this.mediaRecorder.state !== 'inactive') {
      this.mediaRecorder.onstop = null;
      this.mediaRecorder.stop();
    }
    this.cleanup();
  }

  private startVisualizer(): void {
    if (this.visualizer || !this.canvas || !this.audioProcessor) return;
    if (!this.recordingState$.value.isRecording) return;
    const processor = this.audioProcessor;
    const canvas = this.canvas;
    // Animation frames outside the zone: 60 change-detection passes a second
    // for a canvas Angular knows nothing about would be pure waste.
    this.ngZone.runOutsideAngular(() => {
      this.visualizer = new WaveformVisualizer(canvas, () => processor.getVoiceLevel());
      this.visualizer.start();
    });
  }

  private startRecordingTimer(): void {
    this.recordingTimer = setInterval(() => {
      const elapsed = Math.floor((Date.now() - this.recordingStartTime) / 1000);
      if (elapsed !== this.recordingState$.value.duration) {
        this.recordingState$.next({ isRecording: true, duration: elapsed });
      }
    }, 250);
  }

  private createAudioContext(): void {
    try {
      const Ctor = window.AudioContext || (window as any).webkitAudioContext;
      if (!Ctor) return;
      this.audioContext = new Ctor();
      void this.audioContext.resume?.().catch(() => undefined);
    } catch (error) {
      console.error('Error creating audio context:', error);
      this.audioContext = null;
    }
  }

  private connectAnalyser(stream: MediaStream): void {
    if (!this.audioContext) return;
    try {
      const analyser = this.audioContext.createAnalyser();
      this.audioContext.createMediaStreamSource(stream).connect(analyser);
      this.audioProcessor = new VoiceAudioProcessor(analyser);
      void this.audioContext.resume?.().catch(() => undefined);
    } catch (error) {
      console.error('Error setting up audio processing:', error);
    }
  }

  private getSupportedMimeType(): string {
    const types = [
      'audio/webm;codecs=opus',
      'audio/webm',
      'audio/ogg;codecs=opus',
      'audio/ogg',
      'audio/mp4',
      'audio/mpeg',
    ];
    for (const type of types) {
      if (MediaRecorder.isTypeSupported(type)) return type;
    }
    return '';
  }

  private cleanup(): void {
    if (this.recordingTimer) {
      clearInterval(this.recordingTimer);
      this.recordingTimer = null;
    }
    // Stops drawing but keeps the last frame: the composer shows it dimmed
    // while the clip transcribes.
    this.visualizer?.destroy();
    this.visualizer = null;
    if (this.audioStream) {
      this.audioStream.getTracks().forEach((t) => t.stop());
      this.audioStream = null;
    }
    if (this.audioContext) {
      void this.audioContext.close().catch(() => undefined);
      this.audioContext = null;
      this.audioProcessor = null;
    }
    this.recordingState$.next({ isRecording: false, duration: 0 });
    this.mediaRecorder = null;
    this.audioChunks = [];
  }
}
