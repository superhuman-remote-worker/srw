import {describe, expect, it} from 'vitest';
import {TemplateRef, signal} from '@angular/core';
import {RailTakeoverService} from './rail-takeover.service';

const tpl = (name: string) => ({name}) as unknown as TemplateRef<unknown>;

describe('RailTakeoverService', () => {
  it('lends nothing until a page claims the rail', () => {
    expect(new RailTakeoverService().template()).toBeNull();
  });

  it('shows a claimed template while its condition holds', () => {
    const service = new RailTakeoverService();
    const active = signal(true);
    const list = tpl('list');
    service.claim(list, active);
    expect(service.template()).toBe(list);
    active.set(false);
    expect(service.template()).toBeNull();
    active.set(true);
    expect(service.template()).toBe(list);
  });

  it('claims unconditionally when no condition is given', () => {
    const service = new RailTakeoverService();
    const list = tpl('list');
    service.claim(list);
    expect(service.template()).toBe(list);
  });

  it('releases only the claimant', () => {
    const service = new RailTakeoverService();
    const leaving = tpl('leaving');
    const arriving = tpl('arriving');
    service.claim(leaving);
    service.claim(arriving);
    service.release(leaving);
    expect(service.template()).toBe(arriving);
    service.release(arriving);
    expect(service.template()).toBeNull();
  });
});
