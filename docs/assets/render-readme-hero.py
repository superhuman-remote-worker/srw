"""Render the editable HTML banner to the PNG used in the public README.

Requires Playwright and Chromium; see README.md in this directory.
"""
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    assets = Path(__file__).resolve().parent
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(
            viewport={"width": 1200, "height": 480}, device_scale_factor=2
        )
        page.goto((assets / "readme-hero.html").as_uri())
        page.evaluate("""async () => {
            await document.fonts.ready;
            await Promise.all([...document.images].map(image => image.decode()));
        }""")
        page.screenshot(path=str(assets / "readme-hero.png"))
        browser.close()
    print("Rendered docs/assets/readme-hero.png (2400 × 960)")


if __name__ == "__main__":
    main()
