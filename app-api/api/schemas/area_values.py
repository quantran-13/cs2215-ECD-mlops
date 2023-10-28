from pydantic import BaseModel


class UniqueArea(BaseModel):
    values: list[int]
