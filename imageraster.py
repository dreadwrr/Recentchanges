from PIL import Image, ImageFilter, ImageOps
# 12/02/2025


def append_text(logger, msg):
    if logger:
        logger.appendPlainText(msg)
    else:
        print(msg)


def raised_image(input_path, output_path, logger=None):
    # default crest style
    try:
        logo = Image.open(input_path)
    except Exception as e:
        append_text(logger, f"Error opening image: {e}")
        return

    # If image has alpha, separate it
    if logo.mode == 'RGBA':
        r, g, b, a = logo.split()
        rgb_image = Image.merge("RGB", (r, g, b))
    else:
        rgb_image = logo

    # Convert to grayscale
    grayscale_rgb_image = rgb_image.convert("L")

    # EMBOSS for raised effect (no invert)
    raised_logo = grayscale_rgb_image.filter(ImageFilter.EMBOSS())

    # Slightly enhance contrast for sharper raised effect
    raised_logo = ImageOps.autocontrast(raised_logo)

    # Convert back to RGB for merging
    raised_logo_rgb = raised_logo.convert("RGB")

    if logo.mode == "RGBA":
        # Re-merge RGB + original alpha
        raised_logo_rgba = Image.merge("RGBA", (raised_logo_rgb.split() + (a,)))
        raised_logo_rgba.save(output_path)
    else:
        raised_logo_rgb.save(output_path)

    append_text(logger, f"Raised image with transparency saved as {output_path}")


def sunken_image(input_path, output_path, logger=None):
    try:
        logo = Image.open(input_path)
    except Exception as e:
        append_text(logger, f"Error opening image: {e}")
        return

    # Check if the image has an alpha channel (transparency)
    if logo.mode == 'RGBA':
        # Split the image into RGB and Alpha channels
        r, g, b, a = logo.split()
        rgb_image = Image.merge('RGB', (r, g, b))  # Create RGB version for embossing
    else:
        rgb_image = logo

    grayscale_rgb_image = rgb_image.convert("L")

    embossed_logo = grayscale_rgb_image.filter(ImageFilter.EMBOSS())

    sunken_logo = ImageOps.invert(embossed_logo)

    sunken_logo_rgb = sunken_logo.convert("RGB")

    # If the image had transparency, we merge it back with the alpha channel
    if logo.mode == 'RGBA':
        # Re-merge the RGB channels with the original alpha channel
        sunken_logo_rgba = Image.merge('RGBA', (sunken_logo_rgb.split() + (a,)))
        sunken_logo_rgba.save(output_path)
    else:
        # If no transparency, save as a standard RGB image
        sunken_logo_rgb.save(output_path)

    append_text(logger, f"Sunken image with transparency saved as {output_path}")


if __name__ == '__main__':
    # Input and output paths
    # input_image_path = 'mainlogo.png' #  # Replace with your input file path
    # output_image_path = 'proteus.png'  # Output file path

    # Convert to sunken effect (grayscale + emboss + invert) and preserve transparency
    # sunken_image(input_image_path, output_image_path)

    input_image_path = 'Icons8.png'
    output_image_path = 'raised_logo.png'

    raised_image(input_image_path, output_image_path)
