from collections.abc import Callable
from dataclasses import dataclass
from json import loads as json_loads
from typing import Any, Generic, TypeVar, cast

from pydantic import BaseModel, Field
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

    def model_post_init(self, __context: Any) -> None:
        self._init_cloud_binding()

    @classmethod
    def _field_factory_name(cls) -> str:
        return "CloudEmailField"


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
    return cast(FieldInfo, field_info)
