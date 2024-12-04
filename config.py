from pydantic import BaseModel
from typing import List

class Config(BaseModel):
    features: List[str]
    miro_path: str
    features_path: str
    server: str
