from __future__ import annotations

from passlib.context import CryptContext

MIN_PASSWORD_LENGTH = 6
MAX_PASSWORD_BYTES = 4096


class PasswordValidationError(ValueError):
    """Ошибка валидации пароля для UI-обработки."""


def _utf8_size(value: str) -> int:
    return len(value.encode("utf-8"))


def validate_password_for_auth(password: str) -> None:
    """Проверяет пароль без изменения исходной строки."""
    if not isinstance(password, str):
        raise PasswordValidationError("Пароль должен быть строкой.")

    if password == "":
        raise PasswordValidationError("Пароль не может быть пустым.")

    if len(password) < MIN_PASSWORD_LENGTH:
        raise PasswordValidationError(f"Пароль должен содержать минимум {MIN_PASSWORD_LENGTH} символов.")

    password_bytes = _utf8_size(password)
    if password_bytes > MAX_PASSWORD_BYTES:
        raise PasswordValidationError(
            "Пароль слишком длинный. Используйте пароль до 4096 байт в UTF-8."
        )


pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


def hash_password(password: str) -> str:
    validate_password_for_auth(password)
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    if not password_hash:
        return False

    validate_password_for_auth(password)

    try:
        return pwd_context.verify(password, password_hash)
    except Exception:
        return False
