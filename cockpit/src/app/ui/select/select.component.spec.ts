import {afterEach, describe, expect, it, vi} from 'vitest';
import {syncOnOptionChange} from './select.component';

/**
 * Plain DOM, shaped like AppSelectComponent's template: the customizable
 * select's trigger first, then the projected options. The component itself is
 * not mounted — this vitest pipeline does not compile signal inputs or
 * `viewChild`, so a host binding would prove nothing.
 */
function selectWithTrigger(): HTMLSelectElement {
  const el = document.createElement('select');
  const trigger = document.createElement('button');
  trigger.className = 'app-select__trigger';
  trigger.appendChild(document.createElement('selectedcontent'));
  el.appendChild(trigger);
  document.body.appendChild(el);
  return el;
}

function addOption(el: HTMLSelectElement, value: string, label = value): HTMLOptionElement {
  const option = document.createElement('option');
  option.value = value;
  option.textContent = label;
  el.appendChild(option);
  return option;
}

const flush = () => new Promise<void>((resolve) => queueMicrotask(resolve));

describe('syncOnOptionChange', () => {
  let observer: MutationObserver | null = null;

  afterEach(() => {
    observer?.disconnect();
    document.body.replaceChildren();
  });

  it('re-applies the model value once late options render', async () => {
    const el = selectWithTrigger();
    el.value = 'default'; // Applied while the option list is still empty.
    observer = syncOnOptionChange(el, () => 'default');

    for (const family of ['claude-fable', 'claude-opus', 'default', 'gpt-5']) {
      addOption(el, family);
    }
    expect(el.value).toBe('claude-fable'); // The browser's first-option pick.
    await flush();

    expect(el.value).toBe('default');
  });

  it('clears the browser pick when no option matches the model', async () => {
    // An unset provider must not look selected just because a list arrived.
    const el = selectWithTrigger();
    observer = syncOnOptionChange(el, () => null);

    addOption(el, 'endpoint:fixture');
    addOption(el, 'system:openai');
    expect(el.selectedIndex).toBe(0);
    await flush();

    expect(el.selectedIndex).toBe(-1);
  });

  it('re-assigns when a selected option label is filled in after insertion', async () => {
    const el = selectWithTrigger();
    // Angular creates the label's text node empty and interpolates it later.
    const option = addOption(el, 'endpoint:searx', '');
    const label = option.appendChild(document.createTextNode(''));
    observer = syncOnOptionChange(el, () => 'endpoint:searx');
    const setter = vi.spyOn(el, 'value', 'set');

    label.nodeValue = 'SearXNG (endpoint)';
    await flush();

    expect(setter).toHaveBeenCalledWith('endpoint:searx');
  });

  it('ignores its own trigger so re-assignment cannot loop', async () => {
    const el = selectWithTrigger();
    addOption(el, 'a');
    observer = syncOnOptionChange(el, () => 'a');
    const setter = vi.spyOn(el, 'value', 'set');

    el.querySelector('selectedcontent')!.textContent = 'a';
    await flush();

    expect(setter).not.toHaveBeenCalled();
  });
});
