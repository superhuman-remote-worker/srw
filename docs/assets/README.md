# Repository banner

`readme-hero.png` is the 2400 × 960 banner displayed in the public README.
Its editable source is `readme-hero.html`, a fixed 1200 × 480 composition
rendered at twice the pixel density. It uses the accepted sales-site
Travertine palette, typefaces, angled temple and product positioning.

## Edit and render

Edit the HTML/CSS, then run from the repository root:

```sh
python3 -m venv /tmp/srw-banner-venv
/tmp/srw-banner-venv/bin/pip install playwright==1.58.0
/tmp/srw-banner-venv/bin/python -m playwright install chromium
/tmp/srw-banner-venv/bin/python docs/assets/render-readme-hero.py
```

The renderer waits for local fonts and the temple image before capturing
the PNG. No website deployment or private repository is needed. Review
the result at the roughly 1000 px width used by GitHub, as well as on
mobile. The main headline carries the message at small sizes; the README
also provides the positioning and description as accessible text.

## Assets and history

`brand/` contains unchanged copies of the accepted SRW temple WebP and
self-hosted Inter, Cinzel and JetBrains Mono fonts from the sales site.
The corresponding font licenses are included. The source artwork and
earlier design iterations remain preserved in the sales-site repository.

The previous dark banner is preserved in Git history: use
`git log -- docs/assets/readme-hero.png` to find its revision. This banner
is distinct from the website's Open Graph card and GitHub's repository
Social preview setting; those remain separate updates.
