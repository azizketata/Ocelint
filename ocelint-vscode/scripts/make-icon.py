"""Generate icon.png for the OCELint VS Code extension.

Run from the extension directory:
    python scripts/make-icon.py

Produces icon.png (128x128) in the extension root. Requires Pillow.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

SIZE = 128
BG_COLOR = (15, 76, 92, 255)       # deep teal
ACCENT_COLOR = (16, 185, 129, 255)  # emerald
TEXT_COLOR = (255, 255, 255, 255)
SHADOW_COLOR = (0, 0, 0, 80)


def find_bold_font(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/segoeuib.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def main() -> None:
    img = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Rounded square background with a subtle drop-shadow.
    radius = 24
    shadow_offset = 2
    draw.rounded_rectangle(
        (shadow_offset, shadow_offset, SIZE - 1, SIZE - 1),
        radius=radius,
        fill=SHADOW_COLOR,
    )
    draw.rounded_rectangle(
        (0, 0, SIZE - 1 - shadow_offset, SIZE - 1 - shadow_offset),
        radius=radius,
        fill=BG_COLOR,
    )

    # Bold "OC" centered.
    font = find_bold_font(64)
    text = "OC"
    bbox = draw.textbbox((0, 0), text, font=font, anchor="lt")
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    tx = (SIZE - tw) // 2 - bbox[0]
    ty = (SIZE - th) // 2 - bbox[1] - 4  # nudge up to leave room for the badge
    draw.text((tx, ty), text, font=font, fill=TEXT_COLOR)

    # Green checkmark badge in the bottom-right corner.
    badge_r = 18
    cx = SIZE - badge_r - 8
    cy = SIZE - badge_r - 8
    draw.ellipse(
        (cx - badge_r, cy - badge_r, cx + badge_r, cy + badge_r),
        fill=ACCENT_COLOR,
    )
    # Checkmark inside the badge.
    draw.line(
        [(cx - 8, cy + 1), (cx - 2, cy + 7), (cx + 9, cy - 6)],
        fill=(255, 255, 255, 255),
        width=4,
        joint="curve",
    )

    out = Path(__file__).resolve().parent.parent / "icon.png"
    img.save(out, "PNG")
    print(f"wrote {out} ({img.size[0]}x{img.size[1]})")


if __name__ == "__main__":
    main()
