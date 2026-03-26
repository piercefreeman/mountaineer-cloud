from collections.abc import Callable
from dataclasses import dataclass
from json import dumps as json_dumps, loads as json_loads
from typing import Any, Generic, TypeVar, cast

from pydantic import BaseModel, Field, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined, PydanticUndefinedType

from mountaineer_cloud.primitives.base import (
    CloudFieldDefinitionBase,
    CloudValueBase,
)
from mountaineer_cloud.providers_common.email import (
    EmailBody,
    EmailProviderCore,
    EmailRecipient,
)

TEmailCore = TypeVar("TEmailCore", bound=EmailProviderCore[Any])


@dataclass(frozen=True)
class CloudEmailFieldDefinition(CloudFieldDefinitionBase):
    field_factory_name = "CloudEmailField"

    @property
    def primitive_type(self) -> type["EmailMessage[Any]"]:
        return EmailMessage

    def coerce_field_value(
        self,
        value: Any,
        *,
        owner: Any,
        field_name: str,
    ) -> Any:
        if isinstance(value, str):
            value = json_loads(value)

        if isinstance(value, dict):
            return EmailMessage.model_validate(value).bind(
                definition=self,
                owner=owner,
                field_name=field_name,
            )

        return value


class EmailMessage(BaseModel, CloudValueBase[TEmailCore], Generic[TEmailCore]):
    sender: EmailRecipient
    recipient: EmailRecipient
    subject: str
    body: EmailBody

    async def send(self, core: TEmailCore) -> str:
        return await core.email_send(
            sender=self.sender,
            recipient=self.recipient,
            subject=self.subject,
            body=self.body,
        )

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        normalized = dict(value)

        if "recipient" not in normalized and "to" in normalized:
            to_value = normalized.pop("to")
            if isinstance(to_value, list):
                if len(to_value) != 1:
                    raise ValueError(
                        "EmailMessage only supports a single `recipient`."
                    )
                normalized["recipient"] = to_value[0]
            else:
                normalized["recipient"] = to_value

        if "body" not in normalized and (
            "text" in normalized or "html" in normalized
        ):
            normalized["body"] = {
                "text": normalized.pop("text", None),
                "html": normalized.pop("html", None),
            }

        return normalized

    def model_post_init(self, __context: Any) -> None:
        self._init_cloud_binding()

    @classmethod
    def _field_factory_name(cls) -> str:
        return "CloudEmailField"


def _serialize_email_field_for_db(value: Any):
    if value is None:
        return None

    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")

    return json_dumps(value)


def CloudEmailField(
    *,
    default: Any = None,
    default_factory: Callable[[], Any]
    | None
    | PydanticUndefinedType = PydanticUndefined,
    **kwargs: Any,
) -> FieldInfo:
    field_factory = Field

    try:
        from iceaxe import Field as IceaxeField
        from iceaxe.sql_types import ColumnType
    except ImportError:
        pass
    else:
        field_factory = IceaxeField
        if kwargs.get("is_json") is False:
            raise ValueError("CloudEmailField requires `is_json=True` with Iceaxe.")
        kwargs["is_json"] = True
        if "explicit_type" not in kwargs:
            kwargs["explicit_type"] = ColumnType.JSON

    definition = CloudEmailFieldDefinition()

    if default_factory is not PydanticUndefined:
        field_info = field_factory(
            default_factory=default_factory,
            **kwargs,
        )
    else:
        field_info = field_factory(
            default=default,
            **kwargs,
        )

    field_info.metadata.append(definition)
    if hasattr(field_info, "to_db_value"):
        field_info.to_db_value = _serialize_email_field_for_db

    return cast(FieldInfo, field_info)
