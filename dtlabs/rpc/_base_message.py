from abc import ABC, abstractmethod

import json
from pydantic import BaseModel


class Message(ABC, BaseModel):
    def dumps(self):
        return json.dumps(self.json())
