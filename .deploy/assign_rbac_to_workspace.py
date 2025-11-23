from pathlib import Path
import os


def load_env_config(env:str):
    root = Path(__file__).resolve().parents[1]
    env_config_path = os.path.join(root, f"config/env.{env.lower()}.yml")