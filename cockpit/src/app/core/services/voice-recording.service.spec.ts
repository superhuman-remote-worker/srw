import {NgZone} from '@angular/core';
import {firstValueFrom} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {VoiceRecordingService} from './voice-recording.service';

/** A stream whose tracks record whether they were stopped. */
function fakeStream() {
    const track = {stop: vi.fn()};
    return {track, stream: {getTracks: () => [track]} as unknown as MediaStream};
}

class FakeRecorder {
    static isTypeSupported = () => true;
    state: 'inactive' | 'recording' = 'inactive';
    mimeType = 'audio/webm';
    ondataavailable: ((e: {data: Blob}) => void) | null = null;
    onstop: (() => void) | null = null;
    start() {
        this.state = 'recording';
    }
    stop() {
        this.state = 'inactive';
        this.ondataavailable?.({data: new Blob(['x'])});
        this.onstop?.();
    }
}

class FakeAudioContext {
    resume = vi.fn(async () => undefined);
    close = vi.fn(async () => undefined);
    createAnalyser() {
        return {fftSize: 0, smoothingTimeConstant: 0, getByteTimeDomainData: () => undefined};
    }
    createMediaStreamSource() {
        return {connect: () => undefined};
    }
}

describe('VoiceRecordingService', () => {
    let getUserMedia: ReturnType<typeof vi.fn>;
    const zone = {runOutsideAngular: (fn: () => unknown) => fn()} as unknown as NgZone;

    beforeEach(() => {
        getUserMedia = vi.fn();
        vi.stubGlobal('MediaRecorder', FakeRecorder);
        vi.stubGlobal('AudioContext', FakeAudioContext);
        Object.defineProperty(navigator, 'mediaDevices', {
            configurable: true,
            value: {getUserMedia},
        });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('a double tap opens the microphone once', async () => {
        const {stream} = fakeStream();
        let grant!: (s: MediaStream) => void;
        getUserMedia.mockReturnValue(new Promise<MediaStream>((r) => (grant = r)));
        const service = new VoiceRecordingService(zone);

        const first = service.startRecording();
        const second = service.startRecording();
        expect(service.isStarting()).toBe(true);
        grant(stream);
        await Promise.all([first, second]);

        expect(getUserMedia).toHaveBeenCalledOnce();
        expect((await firstValueFrom(service.getRecordingState())).isRecording).toBe(true);
        service.cancelRecording();
    });

    it('a cancel during the permission prompt releases the microphone it is later granted', async () => {
        const {stream, track} = fakeStream();
        let grant!: (s: MediaStream) => void;
        getUserMedia.mockReturnValue(new Promise<MediaStream>((r) => (grant = r)));
        const service = new VoiceRecordingService(zone);

        const started = service.startRecording();
        service.cancelRecording();
        grant(stream);
        await started;

        expect(track.stop).toHaveBeenCalled();
        expect(service.isStarting()).toBe(false);
        expect((await firstValueFrom(service.getRecordingState())).isRecording).toBe(false);
    });

    it('stop hands back the clip and releases the microphone', async () => {
        const {stream, track} = fakeStream();
        getUserMedia.mockResolvedValue(stream);
        const service = new VoiceRecordingService(zone);
        await service.startRecording();

        const result = await service.stopRecording();

        expect(result?.blob.size).toBeGreaterThan(0);
        expect(result?.mimeType).toBe('audio/webm');
        expect(track.stop).toHaveBeenCalled();
        expect((await firstValueFrom(service.getRecordingState())).isRecording).toBe(false);
    });

    it('a refused permission leaves nothing running and reports the error', async () => {
        getUserMedia.mockRejectedValue(new DOMException('denied', 'NotAllowedError'));
        const service = new VoiceRecordingService(zone);
        vi.spyOn(console, 'error').mockImplementation(() => undefined);

        await expect(service.startRecording()).rejects.toMatchObject({name: 'NotAllowedError'});
        expect(service.isStarting()).toBe(false);
        expect((await firstValueFrom(service.getRecordingState())).isRecording).toBe(false);
    });
});
