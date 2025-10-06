from uuid import uuid4
from sqlalchemy import Column, UUID, ForeignKey, String, DateTime, Enum, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db.base import Base
from app.core.crypto import encrypt_secret, decrypt_secret  # <- add

EnvEnum = Enum("testnet", "prod", name="env_enum", create_type=True)

class ApiCredential(Base):
    """
    Per-user API creds (encrypted at rest). One user may have multiple (testnet/prod).
    """
    __tablename__ = "api_credentials"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    env = Column(EnvEnum, nullable=False, server_default="testnet")

    api_key_encrypted = Column(String(1024), nullable=False)
    api_secret_encrypted = Column(String(1024), nullable=False)

    label = Column(String(64), nullable=False, default="default")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # relations
    user = relationship("User", back_populates="creds")
    bots = relationship("Bot", back_populates="credential")

    __table_args__ = (
        Index(
            "uq_cred_owner_scope",
            "user_id", "env", "label",
            unique=True
        ),
    )

    # ---------- convenience (never persist plaintext) ----------
    def set_secrets(self, api_key: str, api_secret: str) -> None:
        """Encrypt and assign both secrets."""
        self.api_key_encrypted = encrypt_secret(api_key)
        self.api_secret_encrypted = encrypt_secret(api_secret)

    def get_decrypted(self) -> tuple[str, str]:
        """Return (api_key, api_secret) decrypted for in-memory use only."""
        return (
            decrypt_secret(self.api_key_encrypted),
            decrypt_secret(self.api_secret_encrypted),
        )
