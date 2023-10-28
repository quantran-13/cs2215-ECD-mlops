from pydantic import BaseModel


class UniqueConsumerType(BaseModel):
    values: list[int]
