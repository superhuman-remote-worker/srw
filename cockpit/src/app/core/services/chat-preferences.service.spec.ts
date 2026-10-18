import {beforeEach, describe, expect, it} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {ChatPreferencesService} from './chat-preferences.service';

const KEY = 'cockpit:chat:reasoningExpanded';
const TOOL_KEY = 'cockpit:chat:toolCallsExpanded';
const WIDTH_KEY = 'cockpit:chat:readingWidth';
const SIZE_KEY = 'cockpit:chat:textSize';
const CLOUD_KEY = 'cockpit:chat:cloudFoldersFolded';

describe('ChatPreferencesService', () => {
  beforeEach(() => {
    TestBed.resetTestingModule();
    window.localStorage.clear();
  });

  it('defaults reasoningExpanded to true when nothing is stored', () => {
    const svc = TestBed.inject(ChatPreferencesService);
    expect(svc.reasoningExpanded()).toBe(true);
  });

  it('reads a stored "false" preference at construction', () => {
    window.localStorage.setItem(KEY, 'false');
    const svc = TestBed.inject(ChatPreferencesService);
    expect(svc.reasoningExpanded()).toBe(false);
  });

  it('reads a stored "true" preference at construction', () => {
    window.localStorage.setItem(KEY, 'true');
    const svc = TestBed.inject(ChatPreferencesService);
    expect(svc.reasoningExpanded()).toBe(true);
  });

  it('falls back to the default (true) for a malformed stored value', () => {
    window.localStorage.setItem(KEY, 'garbage');
    const svc = TestBed.inject(ChatPreferencesService);
    expect(svc.reasoningExpanded()).toBe(true);
  });

  it('updates the signal and persists on set', () => {
    const svc = TestBed.inject(ChatPreferencesService);

    svc.setReasoningExpanded(false);
    expect(svc.reasoningExpanded()).toBe(false);
    expect(window.localStorage.getItem(KEY)).toBe('false');

    svc.setReasoningExpanded(true);
    expect(svc.reasoningExpanded()).toBe(true);
    expect(window.localStorage.getItem(KEY)).toBe('true');
  });

  it('round-trips a persisted value into a fresh service instance', () => {
    TestBed.inject(ChatPreferencesService).setReasoningExpanded(false);
    TestBed.resetTestingModule(); // new injector → new service, same localStorage
    expect(TestBed.inject(ChatPreferencesService).reasoningExpanded()).toBe(false);
  });

  it('defaults toolCallsExpanded to false (folded) when nothing is stored', () => {
    expect(TestBed.inject(ChatPreferencesService).toolCallsExpanded()).toBe(false);
  });

  it('reads a stored "true" toolCallsExpanded preference', () => {
    window.localStorage.setItem(TOOL_KEY, 'true');
    expect(TestBed.inject(ChatPreferencesService).toolCallsExpanded()).toBe(true);
  });

  it('persists toolCallsExpanded independently of reasoningExpanded', () => {
    const svc = TestBed.inject(ChatPreferencesService);
    svc.setToolCallsExpanded(true);
    expect(svc.toolCallsExpanded()).toBe(true);
    expect(window.localStorage.getItem(TOOL_KEY)).toBe('true');
    // reasoning untouched (still its default)
    expect(svc.reasoningExpanded()).toBe(true);
    expect(window.localStorage.getItem(KEY)).toBeNull();
  });

  // --- readingWidth (Comfortable / Wide / Full) ---

  it('defaults readingWidth to "comfortable" when nothing is stored', () => {
    expect(TestBed.inject(ChatPreferencesService).readingWidth()).toBe('comfortable');
  });

  it('reads a stored readingWidth preference at construction', () => {
    window.localStorage.setItem(WIDTH_KEY, 'full');
    expect(TestBed.inject(ChatPreferencesService).readingWidth()).toBe('full');
  });

  it('falls back to "comfortable" for a malformed readingWidth value', () => {
    window.localStorage.setItem(WIDTH_KEY, 'ginormous');
    expect(TestBed.inject(ChatPreferencesService).readingWidth()).toBe('comfortable');
  });

  it('updates the signal and persists readingWidth on set', () => {
    const svc = TestBed.inject(ChatPreferencesService);
    svc.setReadingWidth('wide');
    expect(svc.readingWidth()).toBe('wide');
    expect(window.localStorage.getItem(WIDTH_KEY)).toBe('wide');
  });

  it('round-trips a persisted readingWidth into a fresh service instance', () => {
    TestBed.inject(ChatPreferencesService).setReadingWidth('full');
    TestBed.resetTestingModule(); // new injector → new service, same localStorage
    expect(TestBed.inject(ChatPreferencesService).readingWidth()).toBe('full');
  });

  // --- textSize (Small / Medium / Large) ---

  it('defaults textSize to "medium" when nothing is stored', () => {
    expect(TestBed.inject(ChatPreferencesService).textSize()).toBe('medium');
  });

  it('reads a stored textSize preference at construction', () => {
    window.localStorage.setItem(SIZE_KEY, 'large');
    expect(TestBed.inject(ChatPreferencesService).textSize()).toBe('large');
  });

  it('falls back to "medium" for a malformed textSize value', () => {
    window.localStorage.setItem(SIZE_KEY, 'huge');
    expect(TestBed.inject(ChatPreferencesService).textSize()).toBe('medium');
  });

  it('persists textSize independently of readingWidth', () => {
    const svc = TestBed.inject(ChatPreferencesService);
    svc.setTextSize('small');
    expect(svc.textSize()).toBe('small');
    expect(window.localStorage.getItem(SIZE_KEY)).toBe('small');
    // width untouched (still its default)
    expect(svc.readingWidth()).toBe('comfortable');
    expect(window.localStorage.getItem(WIDTH_KEY)).toBeNull();
  });

  // --- cloudFoldersFolded (the notice above the transcript) ---

  it('defaults cloudFoldersFolded to false (open) and round-trips a fold', () => {
    const svc = TestBed.inject(ChatPreferencesService);
    expect(svc.cloudFoldersFolded()).toBe(false);
    svc.setCloudFoldersFolded(true);
    expect(window.localStorage.getItem(CLOUD_KEY)).toBe('true');
    TestBed.resetTestingModule(); // new injector → new service, same localStorage
    expect(TestBed.inject(ChatPreferencesService).cloudFoldersFolded()).toBe(true);
  });
});
