// Regenerate static PWA artwork from the SVG sources. Requires Inkscape and
// ImageMagick (`magick`) on PATH. Run from cockpit: node scripts/generate-pwa-icons.mjs
import {execFileSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const icons = join(root, 'src/assets/icons');
const publicDir = join(root, 'public');
const temp = mkdtempSync(join(tmpdir(), 'srw-pwa-icons-'));
const icon = readFileSync(join(icons, 'icon.svg'), 'utf8');
const manifest = JSON.parse(readFileSync(join(publicDir, 'manifest.webmanifest'), 'utf8'));

function render(svg, path, size) {
  const source = join(temp, 'icon.svg');
  writeFileSync(source, svg);
  execFileSync('inkscape', [source, '--export-type=png', `--export-filename=${path}`,
    `--export-width=${size}`, `--export-height=${size}`], {stdio: 'pipe'});
}

try {
  for (const size of [72, 96, 128, 144, 152, 192, 384, 512]) {
    render(icon, join(icons, `icon-${size}.png`), size);
  }
  render(icon, join(icons, 'apple-touch-icon.png'), 180);

  // Opaque square with the mark inset to fit the maskable-icon safe area.
  const maskable = icon.replace(' rx="115" ry="115"', '')
    .replace('translate(57 57) scale(12.46875)', 'translate(96.8 96.8) scale(9.975)');
  for (const size of [192, 512]) {
    render(maskable, join(icons, `icon-maskable-${size}.png`), size);
  }

  for (const [name, letter] of [['jobs', 'J'], ['create', 'C'], ['sessions', 'S']]) {
    render(`<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96">
      <rect width="96" height="96" rx="22" fill="${manifest.background_color}"/>
      <text x="48" y="61" text-anchor="middle" font-family="Cinzel, Georgia, serif"
        font-size="40" font-weight="700" fill="${manifest.theme_color}">${letter}</text>
    </svg>`, join(icons, `sc-${name}.png`), 96);
  }

  const favicon = readFileSync(join(publicDir, 'favicon.svg'), 'utf8');
  for (const size of [16, 32, 48]) {
    render(favicon, join(publicDir, `favicon-${size}.png`), size);
  }
  execFileSync('magick', [join(publicDir, 'favicon-16.png'), join(publicDir, 'favicon-32.png'),
    join(publicDir, 'favicon-48.png'), join(publicDir, 'favicon.ico')], {stdio: 'pipe'});
} finally {
  rmSync(temp, {recursive: true, force: true});
}
