# app/db/models/__init__.py
# Import each module so its classes are registered on Base.metadata
from .models.user import User            # noqa: F401
from .models.bots import Bot       # or whatever filename you used  # noqa: F401
from .models.credentials import ApiCredential  # noqa: F401
