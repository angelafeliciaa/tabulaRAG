import re
from fastapi import HTTPException


SAFE_DATASET_NAME_MAX_LENGTH = 64
SAFE_DATASET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9 _-]+$")


def sanitize_dataset_name(raw_name: str) -> str:
    without_extension = re.sub(r"\.(csv|tsv)$", "", raw_name.strip(), flags=re.IGNORECASE)
    without_control_chars = re.sub(r"[\x00-\x1f\x7f]", "", without_extension)
    allowed_chars_only = re.sub(r"[^A-Za-z0-9 _-]", "", without_control_chars)
    normalized_spaces = re.sub(r"\s+", " ", allowed_chars_only).strip()
    return normalized_spaces[:SAFE_DATASET_NAME_MAX_LENGTH]


def normalize_dataset_name_or_raise(raw_name: str) -> str:
    normalized = sanitize_dataset_name(raw_name)
    if not normalized:
        raise HTTPException(
            status_code=400,
            detail=(
                "Name cannot be empty. Use letters, numbers, spaces, underscores, or hyphens."
            ),
        )
    if not SAFE_DATASET_NAME_PATTERN.fullmatch(normalized):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid name. Use only letters, numbers, spaces, underscores, or hyphens."
            ),
        )
    return normalized
