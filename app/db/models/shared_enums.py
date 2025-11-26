from sqlalchemy import Enum as SAEnum

# Shared Postgres enum for credential/bot environments to avoid duplicate type creation.
EnvEnum = SAEnum("testnet", "prod", name="env_enum", create_type=True)
