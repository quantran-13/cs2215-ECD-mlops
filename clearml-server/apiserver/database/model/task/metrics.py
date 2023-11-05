from mongoengine import (
    EmbeddedDocument,
    StringField,
    DynamicField,
    LongField,
    EmbeddedDocumentField,
    IntField,
)

from apiserver.database.fields import SafeMapField


class MetricEvent(EmbeddedDocument):
    meta = {
        # For backwards compatibility reasons
        "strict": False,
    }

    metric = StringField(required=True)
    variant = StringField(required=True)
    value = DynamicField(required=True)
    min_value = DynamicField()  # for backwards compatibility reasons
    min_value_iteration = IntField()
    max_value = DynamicField()  # for backwards compatibility reasons
    max_value_iteration = IntField()


class EventStats(EmbeddedDocument):
    meta = {
        # For backwards compatibility reasons
        "strict": False,
    }
    last_update = LongField()


class MetricEventStats(EmbeddedDocument):
    meta = {
        # For backwards compatibility reasons
        "strict": False,
    }
    metric = StringField(required=True)
    event_stats_by_type = SafeMapField(field=EmbeddedDocumentField(EventStats))
